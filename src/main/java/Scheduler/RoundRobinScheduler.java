package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import java.util.logging.Level;
import java.util.ArrayList;

public class RoundRobinScheduler extends Thread{

    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown) {
                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + "Shutting Down Round Robin Scheduler. Job Queue is Empty...");
                SchedulerManager.ShutDown=true;
                break;
            }
            //update agents
            StatusUpdater.updateAgents();
            //update jobs
            //StatusUpdater.updateJobs();

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {

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

                    //sleep
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public boolean placeExecutor(Job currentJob)  {

        int executorCount=0;
        boolean placed=false;
        boolean reserved=false;
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            if(SchedulerUtil.agentList.get(i).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(i).getMem()>=Math.ceil(currentJob.getTotalExecutorMemory()) ){

                //update the available resources in this agent
                SchedulerUtil.agentList.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu()-currentJob.getCoresPerExecutor());
                SchedulerUtil.agentList.get(i).setMem(SchedulerUtil.agentList.get(i).getMem()-Math.ceil(currentJob.getTotalExecutorMemory()));
                SchedulerUtil.agentList.get(i).setUsed(true);

                // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole,currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Unreserved CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" from the default scheduler-role"+" in agent "+SchedulerUtil.agentList.get(i).getId());
                // use http api reserve-method to reserve resources in this agent
                HTTPAPI.RESERVE(currentJob.getRole(),currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Reserved CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" to Job: "+currentJob.getJobID()+" with Role: "+currentJob.getRole()+" in agent "+SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Current Status of Agent: "+ SchedulerUtil.agentList.get(i).getId()+"-> CPU: "+ SchedulerUtil.agentList.get(i).getCpu()+" Mem: "+ SchedulerUtil.agentList.get(i).getMem());
                //add agent Id to the job
                ArrayList<String> temp = currentJob.getAgentList();
                temp.add(SchedulerUtil.agentList.get(i).getId());
                currentJob.setAgentList(temp);

                executorCount++;
                placed = true;
                reserved=true;
            }
            // all the executors for the current job is placed
            if((executorCount+currentJob.getAllocatedExecutors())==currentJob.getExecutors()) {
                currentJob.setAllocatedExecutors(currentJob.getExecutors());
                break;
            }
            // we have traversed all the agents, now set the index to start so that executors are placed in a round-robin fashion
            if(i==SchedulerUtil.agentList.size()-1) {
                i=-1;
                if(reserved) {
                    reserved = false;
                }
                // no more executors can be placed in any agent
                else{
                    currentJob.setAllocatedExecutors(currentJob.getAllocatedExecutors()+executorCount);
                    break;
                }
            }
        }
        return placed;
    }
}
