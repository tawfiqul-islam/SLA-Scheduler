package Scheduler;

import Entity.Agent;
import Entity.Job;
import JobMananger.SparkLauncherAPI;

import java.util.ArrayList;
import java.util.logging.Level;

public class GIOScheduler extends Thread {


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
                                if (placeExecutor(currentJob,this.getClass())) {

                                    Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + ": Placed executor(s) for Job: " + currentJob.getJobID());
                                    currentJob.setResourceReserved(true);
                                    currentJob.setSchedulingDelay(currentJob.getSchedulingDelay()+SchedulerUtil.placementTime);

                                    if (currentJob.getAllocatedExecutors() == currentJob.getExecutors()) {
                                        Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + ": All executors are placed for Job: " + currentJob.getJobID());
                                        //remove job from job queue
                                        SchedulerUtil.jobQueue.remove(i);
                                        Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                        //add job to fully submitted job list
                                        SchedulerUtil.fullySubmittedJobList.add(currentJob);
                                        Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + ": Added Job: " + currentJob.getJobID() + " to fullySubmittedJobList");
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
                                    Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + ": Submitting Job: " + currentSubmittedJob.getJobID() +" with role: "+currentSubmittedJob.getRole()+ " to the Cluster");
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
                Log.SchedulerLogging.log(Level.INFO, GIOScheduler.class.getName() + "Shutting Down FirstFitDecreasing Scheduler. Job Queue is Empty...");
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

    static boolean placeExecutor(Job currentJob, Class classVar)  {

        int executorCount=0;
        boolean fullyPlaced=false;
        ArrayList<Agent> placedAgents = new ArrayList<>();
        SchedulerUtil.placementTime=System.currentTimeMillis();

        while(true) {

            boolean placementFound=false;
            double currentCost=1000000.0;
            int vmIndex=0;
            for(int i=0;i<SchedulerUtil.agentList.size();i++) {

                    if (SchedulerUtil.agentList.get(i).getCpu() >= currentJob.getCoresPerExecutor() &&
                            SchedulerUtil.agentList.get(i).getMem() >= Math.ceil(currentJob.getTotalExecutorMemory())) {

                        placementFound=true;
                        double tmpT=SchedulerUtil.agentList.get(i).getMaxT() >= (System.currentTimeMillis()/1000+currentJob.getTotalPredictedTime())?0:(System.currentTimeMillis()/1000+currentJob.getTotalPredictedTime()-SchedulerUtil.agentList.get(i).getMaxT());
                        double tmpCost=SchedulerUtil.agentList.get(i).getPrice()*tmpT;
                        if(tmpCost<currentCost) {
                            currentCost = tmpCost;
                            vmIndex = i;
                        }
                    }
                }

            if(placementFound){
                SchedulerUtil.agentList.get(vmIndex).setCpu(SchedulerUtil.agentList.get(vmIndex).getCpu() - currentJob.getCoresPerExecutor());
                SchedulerUtil.agentList.get(vmIndex).setMem(SchedulerUtil.agentList.get(vmIndex).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                placedAgents.add(SchedulerUtil.agentList.get(vmIndex));

                SchedulerUtil.agentList.get(vmIndex).setPrevMaxT(SchedulerUtil.agentList.get(vmIndex).getMaxT());
                SchedulerUtil.agentList.get(vmIndex).setMaxT(Math.max(SchedulerUtil.agentList.get(vmIndex).getMaxT(),System.currentTimeMillis()/1000+currentJob.getTotalPredictedTime()));
                executorCount++;
            }
            else {
                break;
            }

            // all the executors for the current job is placed
            if (executorCount == currentJob.getExecutors()){
                fullyPlaced=true;
                break;
            }
        }

        // All the executors of the current-job can be placed
        // So Reserve Resources in the Agents
        if (fullyPlaced){
            currentJob.setAllocatedExecutors(currentJob.getExecutors());
            SchedulerUtil.placementTime=System.currentTimeMillis()-SchedulerUtil.placementTime;
            SchedulerUtil.resourceReservation(placedAgents,currentJob,classVar);
            return true;
        }
        //Could not place all the executors, take back the resources in Agent data structure..
        //Also set the initial position of the index
        else {
            for(int i=0;i<placedAgents.size();i++){
                placedAgents.get(i).setCpu(placedAgents.get(i).getCpu() + currentJob.getCoresPerExecutor());
                placedAgents.get(i).setMem(placedAgents.get(i).getMem() + Math.ceil(currentJob.getTotalExecutorMemory()));
                placedAgents.get(i).setMaxT(placedAgents.get(i).getPrevMaxT());
            }
            return false;
        }
    }
}
