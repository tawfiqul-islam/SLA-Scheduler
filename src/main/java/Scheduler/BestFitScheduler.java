package Scheduler;

import Entity.Agent;
import Entity.Job;
import Operator.HTTPAPI;
import JobMananger.SparkLauncherAPI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;

public class BestFitScheduler extends Thread {

    class AgentComparator implements Comparator<Agent> {
        @Override
        public int compare(Agent a, Agent b) {

            if (a.getResourceTotal() < b.getResourceTotal()) {
                return -1;
            } else if (a.getResourceTotal() > b.getResourceTotal()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    class JobComparator implements Comparator<Job> {
        @Override
        public int compare(Job a, Job b) {

            if (a.getResourceSplit() > b.getResourceSplit() ) {
                return -1;
            } else if (a.getResourceSplit()  < b.getResourceSplit() ) {
                return 1;
            } else {
                return 0;
            }
        }
    }


    public void run() {

        boolean shutdown= false;

        while(true) {

            if (shutdown) {
                Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + "Shutting Down Best Fit Scheduler. Job Queue is Empty...");
                SchedulerManager.ShutDown=true;
                break;
            }
            //update agents
            StatusUpdater.updateAgents();

            //sort all the Agents according to increasing resource capacity
            Collections.sort(SchedulerUtil.agentList, new AgentComparator());

            //update jobs
            //StatusUpdater.updateJobs();

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    //sort all the Jobs according to decreasing resource requirements
                    Collections.sort(SchedulerUtil.jobQueue, new JobComparator());

                    Job currentJob;

                    //Log.SchedulerLogging.log(Level.INFO,BestFitScheduler.class.getName()+": Trying to place executors for jobs from JobQueue");

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
                            Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                            //if the job is new submit it in the cluster
                            if (!currentJob.isSubmitted()) {
                                currentJob.setSubmitted(true);
                                new SparkLauncherAPI(currentJob).start();
                            }
                            if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                //remove job from job queue
                                SchedulerUtil.jobQueue.remove(i);
                                Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                //add job to fully submitted job list
                                SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
                                i--;
                            }
                        } else {
                            Log.SchedulerLogging.log(Level.INFO, BestFitScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
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

                    //update the available resources in this agent
                    SchedulerUtil.agentList.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu() - currentJob.getCoresPerExecutor());
                    SchedulerUtil.agentList.get(i).setMem(SchedulerUtil.agentList.get(i).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                    SchedulerUtil.agentList.get(i).setUsed(true);

                    // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                    HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole, currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(i).getId());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Unreserved CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " from the default scheduler-role"+" in agent "+SchedulerUtil.agentList.get(i).getId());
                    // use http api reserve-method to reserve resources in this agent
                    HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), SchedulerUtil.agentList.get(i).getId());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Reserved CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole()+" in agent "+SchedulerUtil.agentList.get(i).getId());
                    Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + " Current Status of Agent: " + SchedulerUtil.agentList.get(i).getId() + "-> CPU: " + SchedulerUtil.agentList.get(i).getCpu() + " Mem: " + SchedulerUtil.agentList.get(i).getMem());
                    //add agent Id to the job
                    ArrayList<String> temp = currentJob.getAgentList();
                    temp.add(SchedulerUtil.agentList.get(i).getId());
                    currentJob.setAgentList(temp);

                    executorCount++;
                    placed = true;
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
        return placed;
    }
}
