package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import Operator.ServerResponse;
import java.util.logging.Level;

public class RoundRobinScheduler extends Thread{

    private static ServerResponse resObj;
    private static int index=0;
    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown) {
                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + "Shutting Down Round Robin Scheduler. Job Queue is Empty...");
                SchedulerManager.ShutDown=true;
                break;
            }
            //update agents
            //StatusUpdater.updateAgents();
            //update jobs
            //StatusUpdater.updateJobs();

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    synchronized (SchedulerUtil.agentList) {

                        Job currentJob;

                        //Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Trying to place executors for jobs from jobQueue");

                        for (int i = 0; i < SchedulerUtil.jobQueue.size(); i++) {

                            currentJob = SchedulerUtil.jobQueue.get(i);

                            //shutDown check
                            if (currentJob.isShutdown()) {
                                if (SchedulerUtil.jobQueue.size() == 1) {
                                    shutdown = true;
                                    break;
                                } else {
                                    continue;
                                }
                            }

                            if (placeExecutor(currentJob)) {
                                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                //if the job is new submit it in the cluster
                                if (!currentJob.isSubmitted()) {
                                    currentJob.setSubmitted(true);
                                    new SparkLauncherAPI(currentJob).start();
                                }
                                if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                    //remove job from job queue
                                    SchedulerUtil.jobQueue.remove(i);
                                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                    //add job to fully submitted job list
                                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
                                    i--;
                                }

                            } else {
                                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
                            }
                        }
                    }

                }
            }
            //sleep
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean placeExecutor(Job currentJob)  {

        int executorCount=0;
        boolean placed=false;
        boolean reserved=false;

        for( ;index<SchedulerUtil.agentList.size();index++) {

            // all the executors for the current job is placed
            if((executorCount+currentJob.getAllocatedExecutors())==currentJob.getExecutors()) {
                currentJob.setAllocatedExecutors(currentJob.getExecutors());
                break;
            }

            if(SchedulerUtil.agentList.get(index).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(index).getMem()>=Math.ceil(currentJob.getTotalExecutorMemory()) ){

                // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                resObj=HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole,currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(index).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Unreserving CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" from the default scheduler-role"+" in agent "+SchedulerUtil.agentList.get(index).getId()+" ServerResponse: "+resObj.getResponseString()+" Status Code: "+resObj.getStatusCode());
                // use http api reserve-method to reserve resources in this agent
                if(resObj.getStatusCode()!=409) {
                    resObj = HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(index).getId());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Reserving CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole() + " in agent " + SchedulerUtil.agentList.get(index).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Current Status of Agent: " + SchedulerUtil.agentList.get(index).getId() + "-> CPU: " + SchedulerUtil.agentList.get(index).getCpu() + " Mem: " + SchedulerUtil.agentList.get(index).getMem());
                    if (resObj.getStatusCode() != 409) {
                        //update the available resources in this agent
                        SchedulerUtil.agentList.get(index).setCpu(SchedulerUtil.agentList.get(index).getCpu() - currentJob.getCoresPerExecutor());
                        SchedulerUtil.agentList.get(index).setMem(SchedulerUtil.agentList.get(index).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                        SchedulerUtil.agentList.get(index).setUsed(true);
                        //add agent Id to the job
                        currentJob.getAgentList().add(SchedulerUtil.agentList.get(index).getId());
                        executorCount++;
                        placed = true;
                        reserved = true;
                    }
                }
            }

            // we have traversed all the agents, now set the index to start so that executors are placed in a round-robin fashion
            if(index==SchedulerUtil.agentList.size()-1) {
                index=-1;
                if(reserved) {
                    reserved = false;
                }
                // no more executors can be placed in any agent
                else{
                    currentJob.setAllocatedExecutors(currentJob.getAllocatedExecutors()+executorCount);
                    index=0;
                    break;
                }
            }
        }
        return placed;
    }
}
