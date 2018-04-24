package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;

import java.util.Collections;
import java.util.logging.Level;

public class FirstFitDeadlineScheduler extends Thread {

    public void run() {

        Job currentJob;
        boolean shutdown= false;
        boolean PriorityJobWaiting=false;

        while(true) {

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.priorityJobQueue) {
                    synchronized (SchedulerUtil.fullySubmittedJobList) {
                        synchronized (SchedulerUtil.agentList) {

                            //Always fetch priority jobs
                            //so that if a higher priority job (a job with having the earliest wall-clock deadline) comes late it can be scheduled earlier than other jobs
                            SchedulerUtil.fetchPriorityJobs();

                            //update agents
                            StatusUpdater.updateAgents();

                            //update jobs
                            StatusUpdater.updateJobs();

                            //sort all the Jobs according to decreasing resource requirements / jobSize
                            Collections.sort(SchedulerUtil.priorityJobQueue, new BestFitDeadlineScheduler.JobComparatorDeadline());

                            for (int i = 0; i < SchedulerUtil.priorityJobQueue.size(); i++) {

                                currentJob = SchedulerUtil.priorityJobQueue.get(i);

                                //shutDown check
                                if (currentJob.isShutdown()) {
                                    if (SchedulerUtil.priorityJobQueue.size() == 1 && SchedulerUtil.jobBuffer.size()==0 && SchedulerUtil.jobQueue.size() == 0 && SchedulerUtil.fullySubmittedJobList.size() == 0) {
                                        shutdown = true;
                                    }
                                } else {
                                    if(BestFitDeadlineScheduler.executorPlacementFind(currentJob, i,1,this.getClass())) {
                                        PriorityJobWaiting=false;
                                        i--;
                                    }
                                    else{
                                        PriorityJobWaiting=true;
                                        break;
                                    }
                                }
                            }

                            if(!PriorityJobWaiting) {
                                //if job_queue is empty, fetch jobs from job_buffer to job_queue
                                //otherwise keep working on the current jobqueue
                                if (SchedulerUtil.jobQueue.size() == 0) {
                                    SchedulerUtil.fetchJobs();
                                }

                                //update agents
                                StatusUpdater.updateAgents();

                                //update jobs
                                StatusUpdater.updateJobs();

                                //sort all the Jobs according to decreasing resource requirements / jobSize
                                Collections.sort(SchedulerUtil.jobQueue, new BestFitScheduler.JobComparator());


                                //Log.SchedulerLogging.log(Level.INFO,FirstFitDScheduler.class.getName()+": Trying to place executors for jobs from JobQueue");

                                for (int i = 0; i < SchedulerUtil.jobQueue.size(); i++) {

                                    currentJob = SchedulerUtil.jobQueue.get(i);

                                    //shutDown check
                                    if (currentJob.isShutdown()) {
                                        if (SchedulerUtil.jobQueue.size() == 1 && SchedulerUtil.priorityJobQueue.size() == 0 && SchedulerUtil.fullySubmittedJobList.size() == 0) {
                                            shutdown = true;
                                        }
                                    } else {
                                        if(BestFitDeadlineScheduler.executorPlacementFind(currentJob, i,2,this.getClass())){
                                            i--;
                                        }
                                    }
                                }

                                //Submitting new jobs (with fully placed executors) to the cluster
                                for (int i = 0; i < SchedulerUtil.fullySubmittedJobList.size(); i++) {
                                    currentJob = SchedulerUtil.fullySubmittedJobList.get(i);
                                    //if the job is new submit it in the cluster
                                    if (!currentJob.isSubmitted() && currentJob.isResourceReserved()) {
                                        currentJob.setSubmitted(true);
                                        Log.SchedulerLogging.log(Level.INFO, FirstFitDeadlineScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() + " with role: " + currentJob.getRole() + " to the Cluster");
                                        new SparkLauncherAPI(currentJob).start();
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (shutdown && SchedulerUtil.fullySubmittedJobList.size() == 0) {
                Log.SchedulerLogging.log(Level.INFO, FirstFitDeadlineScheduler.class.getName() + "Shutting Down FirstFitDecreasing Deadline Scheduler. Job Queue is Empty...");
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
