package Scheduler;

import Entity.Agent;
import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;

public class FirstFitDScheduler extends Thread {

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

    class JobComparator implements Comparator<Job> {
        @Override
        public int compare(Job a, Job b) {

            //to sort jobs in a decreasing order of resource demands (big to small)
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

            if (shutdown&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + "Shutting Down FirstFitDecreasing Scheduler. Job Queue is Empty...");
                SchedulerManager.shutDown();
                break;
            }
            //if job_queue is empty -> lock job_buffer and try to fetch jobs from job_buffer to job_queue

            //otherwise keep working on the current jobqueue
            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    synchronized (SchedulerUtil.agentList) {

                        //update agents
                        StatusUpdater.updateAgents();

                        //sort all the Agents according to decreasing resource capacity
                        //Collections.sort(SchedulerUtil.agentList, new AgentComparator());

                        //update jobs
                        StatusUpdater.updateJobs();

                        //sort all the Jobs according to decreasing resource requirements
                        Collections.sort(SchedulerUtil.jobQueue, new JobComparator());


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
                                if (BestFitScheduler.placeExecutor(currentJob,this.getClass())) {

                                    Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);
                                    currentJob.setSchedulingDelay(currentJob.getSchedulingDelay()+BestFitScheduler.placementTime);

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

                        //Submitting new jobs (with fully placed executors) to the cluster
                        for(int i=0;i<SchedulerUtil.fullySubmittedJobList.size();i++)
                        {
                            currentJob = SchedulerUtil.fullySubmittedJobList.get(i);
                            //if the job is new submit it in the cluster
                            if (!currentJob.isSubmitted()&&currentJob.isResourceReserved()) {
                                currentJob.setSubmitted(true);
                                Log.SchedulerLogging.log(Level.INFO, FirstFitDScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() +" with role: "+currentJob.getRole()+ " to the Cluster");
                                new SparkLauncherAPI(currentJob).start();
                            }
                        }
                    }
                }
            }

            //sleep
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
