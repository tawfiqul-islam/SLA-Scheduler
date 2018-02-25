package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.logging.Level;

public class FirstFitScheduler extends Thread{

    private static ServerResponse resObj;
    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown) {
                Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + "Shutting Down Round Robin Scheduler. Job Queue is Empty...");
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

                        //Log.SchedulerLogging.log(Level.INFO,FirstFitScheduler.class.getName()+": Trying to place executors for jobs from jobQueue");

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
                                Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                //if the job is new submit it in the cluster
                                if (!currentJob.isSubmitted()) {
                                    currentJob.setSubmitted(true);
                                    new SparkLauncherAPI(currentJob).start();
                                }
                                if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                    //remove job from job queue
                                    SchedulerUtil.jobQueue.remove(i);
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                    //add job to fully submitted job list
                                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
                                    i--;
                                }

                            } else {
                                Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
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

        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            if(SchedulerUtil.agentList.get(i).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(i).getMem()>=Math.ceil(currentJob.getTotalExecutorMemory()) ){

                // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                resObj=HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole,currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName()+" Unreserving CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" from the default scheduler-role"+" in agent "+SchedulerUtil.agentList.get(i).getId()+" ServerResponse: "+resObj.getResponseString()+" Status Code: "+resObj.getStatusCode());
                // use http api reserve-method to reserve resources in this agent
                if(resObj.getStatusCode()!=409) {
                    resObj = HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(i).getId());
                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + " Reserving CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole() + " in agent " + SchedulerUtil.agentList.get(i).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());
                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + " Current Status of Agent: " + SchedulerUtil.agentList.get(i).getId() + "-> CPU: " + SchedulerUtil.agentList.get(i).getCpu() + " Mem: " + SchedulerUtil.agentList.get(i).getMem());
                    if (resObj.getStatusCode() != 409) {
                        //update the available resources in this agent
                        SchedulerUtil.agentList.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu() - currentJob.getCoresPerExecutor());
                        SchedulerUtil.agentList.get(i).setMem(SchedulerUtil.agentList.get(i).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                        SchedulerUtil.agentList.get(i).setUsed(true);
                        //add agent Id to the job
                        currentJob.getAgentList().add(SchedulerUtil.agentList.get(i).getId());
                        executorCount++;
                        placed = true;
                        i--;
                    }
                }
            }
            // all the executors for the current job is placed
            if((executorCount+currentJob.getAllocatedExecutors())==currentJob.getExecutors()) {
                currentJob.setAllocatedExecutors(currentJob.getExecutors());
                return placed;
            }
        }
        currentJob.setAllocatedExecutors(currentJob.getAllocatedExecutors()+executorCount);
        return placed;
    }
}
