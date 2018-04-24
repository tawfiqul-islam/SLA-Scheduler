package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;

import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;

public class BestFitDeadlineScheduler extends Thread {

    static class JobComparatorDeadline implements Comparator<Job> {
        @Override
        public int compare(Job a, Job b) {

            //to sort jobs in a increasing order of LowCost Threshold (small to big)
            if (a.getDeadline() < b.getDeadline() ) {
                return -1;
            } else if (a.getDeadline()  > b.getDeadline() ) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public void run() {

        Job currentJob;
        boolean shutdown= false;
        boolean priorityJobWaiting=false;

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
                            Collections.sort(SchedulerUtil.priorityJobQueue, new JobComparatorDeadline());

                            for (int i = 0; i < SchedulerUtil.priorityJobQueue.size(); i++) {

                                //sort all the Agents according to increasing resource capacity
                                Collections.sort(SchedulerUtil.agentList, new BestFitScheduler.AgentComparator());

                                currentJob = SchedulerUtil.priorityJobQueue.get(i);

                                //shutDown check
                                if (currentJob.isShutdown()) {
                                    if (SchedulerUtil.priorityJobQueue.size() == 1 && SchedulerUtil.jobBuffer.size()==0 && SchedulerUtil.jobQueue.size() == 0 && SchedulerUtil.fullySubmittedJobList.size() == 0) {
                                        shutdown = true;
                                    }
                                } else {
                                    if(executorPlacementFind(currentJob, i,1,this.getClass())) {
                                        priorityJobWaiting=false;
                                        i--;
                                    }
                                    else{
                                        priorityJobWaiting=true;
                                        break;
                                    }
                                }
                            }

                            if(!priorityJobWaiting) {
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

                                for (int i = 0; i < SchedulerUtil.jobQueue.size(); i++) {
                                    //sort all the Agents according to increasing resource capacity
                                    Collections.sort(SchedulerUtil.agentList, new BestFitScheduler.AgentComparator());

                                    currentJob = SchedulerUtil.jobQueue.get(i);

                                    //shutDown check
                                    if (currentJob.isShutdown()) {
                                        if (SchedulerUtil.jobQueue.size() == 1 && SchedulerUtil.priorityJobQueue.size() == 0 && SchedulerUtil.fullySubmittedJobList.size() == 0) {
                                            shutdown = true;
                                        }
                                    } else {
                                        if(executorPlacementFind(currentJob, i,2,this.getClass())){
                                            i--;
                                        }
                                    }
                                }
                            }

                            //Submitting new jobs (with fully placed executors) to the cluster
                            for (int i = 0; i < SchedulerUtil.fullySubmittedJobList.size(); i++) {
                                currentJob = SchedulerUtil.fullySubmittedJobList.get(i);
                                //if the job is new submit it in the cluster
                                if (!currentJob.isSubmitted() && currentJob.isResourceReserved()) {
                                    currentJob.setSubmitted(true);
                                    Log.SchedulerLogging.log(Level.INFO, BestFitDeadlineScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() + " with role: " + currentJob.getRole() + " to the Cluster");
                                    new SparkLauncherAPI(currentJob).start();
                                }
                            }
                        }
                    }
                }
            }

            if (shutdown&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                Log.SchedulerLogging.log(Level.INFO, BestFitDeadlineScheduler.class.getName() + "Shutting Down BestFit Scheduler. Job Queue is Empty...");
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

    static boolean executorPlacementFind(Job currentJob, int i, int jobType, Class classVar) {

        if (BestFitScheduler.placeExecutor(currentJob, classVar.getClass())) {

            Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
            currentJob.setResourceReserved(true);
            currentJob.setSchedulingDelay(currentJob.getSchedulingDelay() + BestFitScheduler.placementTime);
            if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ": All executors are placed for Job: " + currentJob.getJobID());

                if(jobType==1) {
                    //remove job from priority job queue
                    SchedulerUtil.priorityJobQueue.remove(i);
                    Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ": Removed Job: " + currentJob.getJobID() + " from PriorityJobQueue");
                }
                else {
                    //remove job from job queue
                    SchedulerUtil.jobQueue.remove(i);
                    Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                }
                //add job to fully submitted job list
                SchedulerUtil.fullySubmittedJobList.add(currentJob);
                Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
            }
            return true;
        } else {
            //could not place any executors for the current job
            //Log.SchedulerLogging.log(Level.INFO, classVar.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
            return false;
        }
    }
}
