package Scheduler;

import Entity.Agent;
import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;

public class FirstFitDScheduler extends Thread {

    private static ServerResponse resObj;

    class AgentComparator implements Comparator<Agent> {
        @Override
        public int compare(Agent a, Agent b) {

            //to sort agents in a decreasing order of resource capacity (big to small)
            if (a.getResourceTotal() > b.getResourceTotal()) {
                return -1;
            } else if (a.getResourceTotal() < b.getResourceTotal()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + "Shutting Down FirstFitDecreasing Scheduler. Job Queue is Empty...");
                SchedulerManager.shutDown();
                break;
            }
            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    synchronized (SchedulerUtil.agentList) {

                        //update agents
                        StatusUpdater.updateAgents();

                        //sort all the Agents according to decreasing resource capacity
                        Collections.sort(SchedulerUtil.agentList, new AgentComparator());

                        //update jobs
                        StatusUpdater.updateJobs();

                        //sleep
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Job currentJob;

                        //Log.SchedulerLogging.log(Level.INFO,FirstFitDScheduler.class.getName()+": Trying to place executors for jobs from JobQueue");

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
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);

                                    if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                        Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                        //remove job from job queue
                                        SchedulerUtil.jobQueue.remove(i);
                                        Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                        //add job to fully submitted job list
                                        SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                        Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
                                        i--;
                                    }
                                } else {
                                    //could not place any executors for the current job
                                    //Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
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
                                Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() +" with role: "+currentJob.getRole()+ " to the Cluster");
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
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            while(true) {

                if (SchedulerUtil.agentList.get(i).getCpu() >= currentJob.getCoresPerExecutor() &&
                        SchedulerUtil.agentList.get(i).getMem() >= Math.ceil(currentJob.getTotalExecutorMemory())) {
                    boolean httpOperation=false;

                    while(true) {
                        // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                        resObj = HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole, currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(i).getId());
                        Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + " Trying to UnReserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " from the default scheduler-role" + " in agent " + SchedulerUtil.agentList.get(i).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());

                        if (resObj.getStatusCode() != 409) {

                            while(true) {
                                // use http api reserve-method to reserve resources in this agent
                                resObj = HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(i).getId());
                                Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + " Trying to Reserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole() + " in agent " + SchedulerUtil.agentList.get(i).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());
                                if (resObj.getStatusCode() != 409) {
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + " *RESERVATION SUCCESSFUL* --> Current Status of Agent: " + SchedulerUtil.agentList.get(i).getId() + "-> CPU: " + SchedulerUtil.agentList.get(i).getCpu() + " Mem: " + SchedulerUtil.agentList.get(i).getMem());
                                    //update the available resources in this agent
                                    SchedulerUtil.agentList.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu() - currentJob.getCoresPerExecutor());
                                    SchedulerUtil.agentList.get(i).setMem(SchedulerUtil.agentList.get(i).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                                    SchedulerUtil.agentList.get(i).setUsed(true);
                                    //add agent Id to the job
                                    currentJob.getAgentList().add(SchedulerUtil.agentList.get(i).getId());
                                    executorCount++;
                                    placed = true;
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
                else {
                    break;
                }
                // all the executors for the current job is placed
                if ((executorCount + currentJob.getAllocatedExecutors()) == currentJob.getExecutors()) {
                    currentJob.setAllocatedExecutors(currentJob.getExecutors());
                    return placed;
                }
            }
        }
        currentJob.setAllocatedExecutors(currentJob.getAllocatedExecutors()+executorCount);
        return placed;
    }
}
