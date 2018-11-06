package Scheduler;

import Entity.Agent;
import Entity.Job;
import JobMananger.SparkLauncherAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;

public class MorpheusScheduler extends Thread{

    private static long placementTime;
    private static int index=0;
    private static double clusterCPULoad;
    private static double clusterMEMLoad;
    private static double clusterCPUCap;
    private static double clusterMEMCap;

    static class JobComparatorLowCost implements Comparator<Job> {
        @Override
        public int compare(Job a, Job b) {

            //to sort jobs in a increasing order of LowCost Threshold (small to big)
            if (a.getLowCostThreshold() < b.getLowCostThreshold() ) {
                return -1;
            } else if (a.getLowCostThreshold()  > b.getLowCostThreshold() ) {
                return 1;
            } else {
                return 0;
            }
        }
    }

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

                        //update jobs
                        StatusUpdater.updateJobs();

                        calculateClusterLoad();
                        setJobsLowCost();
                        //sort all the Jobs according to increasing lowCostThreshold
                        Collections.sort(SchedulerUtil.jobQueue, new JobComparatorLowCost());

                        Job currentJob;

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
                                if (RoundRobinScheduler.placeExecutor(currentJob,this.getClass())) {

                                    Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);
                                    currentJob.setSchedulingDelay(currentJob.getSchedulingDelay()+placementTime);

                                    if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                        Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                        //remove job from job queue
                                        SchedulerUtil.jobQueue.remove(i);
                                        Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                        //add job to fully submitted job list
                                        SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                        Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
                                        i--;
                                    }

                                } else {
                                    //could not place any executors for the current job
                                    //Log.SchedulerLogging.log(Level.INFO, RoundRobinScheduler.class.getName() + ":Could not place any executor(s) for Job: " + currentJob.getJobID());
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
                                Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + ": Submitting Job: " + currentJob.getJobID() +" with role: "+currentJob.getRole()+ " to the Cluster");
                                new SparkLauncherAPI(currentJob).start();
                            }
                        }
                    }
                }
            }

            if (shutdown&&(SchedulerUtil.fullySubmittedJobList.size()==0||(System.currentTimeMillis()-shutdownJobArrivalTime)/1000>=1200)) {
                Log.SchedulerLogging.log(Level.INFO, MorpheusScheduler.class.getName() + "Shutting Down Morpheus Scheduler. Job Queue is Empty...");
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

    private void setJobsLowCost() {
        for(int i=0;i<SchedulerUtil.jobQueue.size();i++)
        {
            SchedulerUtil.jobQueue.get(i).setLowCostThreshold(clusterCPULoad,clusterCPUCap,clusterMEMLoad,clusterMEMCap);
        }
    }

    private void calculateClusterLoad() {

        clusterCPULoad=0;
        clusterMEMLoad=0;
        clusterCPUCap=0;
        clusterMEMCap=0;
        for(int i=0;i<SchedulerUtil.agentList.size();i++)
        {
            clusterCPULoad+=SchedulerUtil.agentList.get(i).getCpu();
            clusterMEMLoad=SchedulerUtil.agentList.get(i).getMem();

            clusterCPUCap+=SchedulerUtil.agentList.get(i).getDefaultCPU();
            clusterMEMCap=SchedulerUtil.agentList.get(i).getDefaultMEM();
        }
    }
/*
    private boolean placeExecutor(Job currentJob, Class classVar)  {

        int executorCount=0,storedIndex=index,lastPlaced=index;

        ArrayList<Agent> placedAgents = new ArrayList<>();
        placementTime=System.currentTimeMillis();

        for(int visited=0 ;index<SchedulerUtil.agentList.size()&&visited<SchedulerUtil.agentList.size();index++,visited++) {

            // all the executors for the current job is placed
            if(executorCount==currentJob.getExecutors()) {
                break;
            }

            if(SchedulerUtil.agentList.get(index).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(index).getMem()>=Math.ceil(currentJob.getTotalExecutorMemory()) ) {

                SchedulerUtil.agentList.get(index).setCpu(SchedulerUtil.agentList.get(index).getCpu() - currentJob.getCoresPerExecutor());
                SchedulerUtil.agentList.get(index).setMem(SchedulerUtil.agentList.get(index).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                placedAgents.add(SchedulerUtil.agentList.get(index));
                executorCount++;
                visited=-1;
                lastPlaced=index;
            }

            // we have traversed all the agents, now set the index to start so that executors are placed in a round-robin fashion
            if(index==SchedulerUtil.agentList.size()-1) {
                index=-1;
            }
        }

        // All the executors of the current-job can be placed
        // So Reserve Resources in the Agents
        if(executorCount==currentJob.getExecutors()) {

            currentJob.setAllocatedExecutors(currentJob.getExecutors());

            if((lastPlaced+1)==SchedulerUtil.agentList.size()) {
                index=0;
            }
            else {
                index=lastPlaced+1;
            }

            placementTime=System.currentTimeMillis()-placementTime;
            SchedulerUtil.resourceReservation(placedAgents,currentJob,classVar.getClass());


            return true;
        }

        //Could not place all the executors, take back the resources in Agent data structure..
        //Also set the initial position of the index
        else {
            index=storedIndex;
            for(int i=0;i<placedAgents.size();i++){
                placedAgents.get(i).setCpu(placedAgents.get(i).getCpu() + currentJob.getCoresPerExecutor());
                placedAgents.get(i).setMem(placedAgents.get(i).getMem() + Math.ceil(currentJob.getTotalExecutorMemory()));
            }
            return false;
        }
    }*/
}

