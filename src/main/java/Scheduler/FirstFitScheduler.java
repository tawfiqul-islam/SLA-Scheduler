package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.Constants;

import java.util.Collections;
import java.util.logging.Level;

public class FirstFitScheduler extends Thread {


    public void run() {

        boolean shutdown= false;
        long shutdownJobArrivalTime=0;

        while(true) {
            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {
                    synchronized (SchedulerUtil.agentList) {

                        //if job_queue is empty, fetch jobs from job_buffer to job_queue
                        //otherwise keep working on the current jobqueue
                        if(SchedulerUtil.jobQueue.size()==0) {
                            SchedulerUtil.fetchJobs();
                        }

                        //update agents
                        StatusUpdater.updateAgents();

                        //update jobs
                        StatusUpdater.updateJobs();

                        if(SchedulerUtil.schedulerAlgorithm!= Algorithm.FFHybrid) {
                            //sort all the Jobs according to decreasing resource requirements / jobSize
                            Collections.sort(SchedulerUtil.jobQueue, new BestFitScheduler.JobComparator());
                        }

                        Job currentJob;

                        //Log.SchedulerLogging.log(Level.INFO,FirstFitScheduler.class.getName()+": Trying to place executors for jobs from JobQueue");

                        for (int i = 0; i < SchedulerUtil.jobQueue.size(); i++) {

                            currentJob = SchedulerUtil.jobQueue.get(i);

                            //shutDown check
                            if (currentJob.isShutdown()) {
                                if (SchedulerUtil.jobQueue.size() == 1&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                                    shutdown = true;
                                    shutdownJobArrivalTime=System.currentTimeMillis();
                                }
                            }
                            else {
                                if (BestFitScheduler.placeExecutor(currentJob,this.getClass())) {

                                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);
                                    currentJob.setSchedulingDelay(currentJob.getSchedulingDelay()+SchedulerUtil.placementTime);

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
                                    //could not place any executors for the current job
                                    //Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
                                }
                            }

                            //Submitting new jobs (with fully placed executors) to the cluster
                            for(int j=0;j<SchedulerUtil.fullySubmittedJobList.size();j++)
                            {
                                Job currentSubmittedJob = SchedulerUtil.fullySubmittedJobList.get(j);
                                //if the job is new submit it in the cluster
                                if (!currentSubmittedJob.isSubmitted()&&currentSubmittedJob.isResourceReserved()) {
                                    currentSubmittedJob.setSubmitted(true);
                                    Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + ": Submitting Job: " + currentSubmittedJob.getJobID() +" with role: "+currentSubmittedJob.getRole()+ " to the Cluster");
                                    new SparkLauncherAPI(currentSubmittedJob).start();
                                    try {
                                        Thread.sleep(3000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }

                                }
                            }
                        }
                    }
                }
            }

            if (shutdown&&(SchedulerUtil.fullySubmittedJobList.size()==0||(System.currentTimeMillis()-shutdownJobArrivalTime)/1000>=1200)) {
                Log.SchedulerLogging.log(Level.INFO, FirstFitScheduler.class.getName() + "Shutting Down FirstFitDecreasing Scheduler. Job Queue is Empty...");
                SchedulerManager.shutDown();
                break;
            }

            //sleep
            try {
                Thread.sleep(SchedulerUtil.schedulingInterval*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
