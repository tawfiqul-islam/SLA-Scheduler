package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import Operator.ServerResponse;
import java.util.logging.Level;

public class RoundRobinScheduler extends Thread{

    private static ServerResponse resObj;
    private static long placementTime;
    private static int index=0;

    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + "Shutting Down Round Robin Scheduler. Job Queue is Empty...");
                SchedulerManager.shutDown();
                break;
            }

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    synchronized (SchedulerUtil.agentList) {

                        //update jobs
                        StatusUpdater.updateJobs();
                        //sleep
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Job currentJob;

                        //Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Trying to place executors for jobs from jobQueue");

                        for (int i = 0; i < SchedulerUtil.jobQueue.size(); i++) {

                            currentJob = SchedulerUtil.jobQueue.get(i);

                            //shutDown check
                            if (currentJob.isShutdown()) {
                                if (SchedulerUtil.jobQueue.size() == 1&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                                    shutdown = true;
                                }
                            }
                            else {
                                if (placeExecutor(currentJob)) {

                                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);
                                    currentJob.setSchedulingDelay(currentJob.getSchedulingDelay()+placementTime);

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
                                    //could not place any executors for the current job
                                    //Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
                                }
                            }


                        }
                        //sleep
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        //Submitting new jobs (with fully placed executors) to the cluster
                        for(int i=0;i<SchedulerUtil.fullySubmittedJobList.size();i++)
                        {
                            currentJob = SchedulerUtil.fullySubmittedJobList.get(i);
                            //if the job is new submit it in the cluster
                            if (!currentJob.isSubmitted()&&currentJob.isResourceReserved()) {
                                currentJob.setSubmitted(true);
                                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() +" with role: "+currentJob.getRole()+ " to the Cluster");
                                new SparkLauncherAPI(currentJob).start();
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
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
        placementTime=System.currentTimeMillis();

        for( ;index<SchedulerUtil.agentList.size();index++) {

            // all the executors for the current job is placed
            if((executorCount+currentJob.getAllocatedExecutors())==currentJob.getExecutors()) {
                currentJob.setAllocatedExecutors(currentJob.getExecutors());
                break;
            }

            if(SchedulerUtil.agentList.get(index).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(index).getMem()>=Math.ceil(currentJob.getTotalExecutorMemory()) ){
                boolean httpOperation=false;
                while(true) {
                    // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                    resObj = HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole, currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(index).getId());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Trying to UnReserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " from the default scheduler-role" + " in agent " + SchedulerUtil.agentList.get(index).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());
                    // use http api reserve-method to reserve resources in this agent
                    if (resObj.getStatusCode() != 409) {

                        while(true) {
                            resObj = HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(index).getId());
                            Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Trying to Reserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole() + " in agent " + SchedulerUtil.agentList.get(index).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());

                            if (resObj.getStatusCode() != 409) {
                                Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + "*RESERVATION SUCCESSFUL* --> Current Status of Agent: " + SchedulerUtil.agentList.get(index).getId() + "-> CPU: " + SchedulerUtil.agentList.get(index).getCpu() + " Mem: " + SchedulerUtil.agentList.get(index).getMem());
                                //update the available resources in this agent
                                SchedulerUtil.agentList.get(index).setCpu(SchedulerUtil.agentList.get(index).getCpu() - currentJob.getCoresPerExecutor());
                                SchedulerUtil.agentList.get(index).setMem(SchedulerUtil.agentList.get(index).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                                SchedulerUtil.agentList.get(index).setUsed(true);
                                //add agent Id to the job
                                currentJob.getAgentList().add(SchedulerUtil.agentList.get(index).getId());
                                executorCount++;
                                placed = true;
                                reserved = true;
                                httpOperation = true;
                                break;
                            }
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    if(httpOperation) {
                        break;
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
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
        placementTime=System.currentTimeMillis()-placementTime;
        return placed;
    }
}
