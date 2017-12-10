package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;
import java.util.logging.Level;
import java.util.ArrayList;

public class RoundRobinScheduler extends Thread{

    public void run() {

        while(true) {

            //update agents, update jobs
            StatusUpdater.updateAgents();
            StatusUpdater.updateJobs();
            Job currentJob;

            //allocate more resources to partial submitted jobs

            for(int i=0;i<SchedulerUtil.partialSubmittedJobList.size();i++) {

                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Trying to place executors for jobs submitted with partial resources");
                currentJob=SchedulerUtil.partialSubmittedJobList.get(i);

                if(placeExecutor(currentJob)) {
                     Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Placed executor(s) for Job: "+currentJob.getJobID());
                }
                else {
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+":Could not place any executor(s) for the Partially Submitted Job: "+currentJob.getJobID());
                }
                if(currentJob.getAllocatedExecutors()==currentJob.getExecutors()) {
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": All executors are placed for Job: "+currentJob.getJobID());
                    //remove job from job queue
                    SchedulerUtil.partialSubmittedJobList.remove(i);
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Removed Job: "+currentJob.getJobID()+" from partialSubmittedJobList");
                    //add job to fully submitted job list
                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Added Job: "+currentJob.getJobID()+" to fullySubmittedJobList");
                    i--;
                }
            }

            //schedule new jobs

            if(!SchedulerUtil.newJobQueue.isEmpty()) {

                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Trying to place executors for new jobs");
                currentJob=SchedulerUtil.queueOperation(null,2);
                if(placeExecutor(currentJob)) {
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Placed executor(s) for Job: "+currentJob.getJobID());
                }
                else {
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+":Could not place any executor(s) for the New Job: "+currentJob.getJobID());
                }
                //job submitted with all the required resource
                if(currentJob.getAllocatedExecutors()==currentJob.getExecutors()) {
                    //remove job from job queue
                    SchedulerUtil.queueOperation(null,3);
                    //add job to fully submitted job list
                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Added Job: "+currentJob.getJobID()+" to fullySubmittedJobList");
                    //launch this new job in the cluster through the sparklauncher API
                    new SparkLauncherAPI(currentJob).start();
                }
                //job submitted with partial resources
                else if(currentJob.getAllocatedExecutors()>0)  {
                    //remove job from job queue
                    SchedulerUtil.queueOperation(null,3);
                    //add job to partial submitted job list
                    SchedulerUtil.partialSubmittedJobList.add(currentJob);
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Added Job: "+currentJob.getJobID()+" to partialSubmittedJobList");
                    Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+": Launching Job: "+currentJob.getJobID()+" in the cluster via SparkLaunchAPI");
                    //launch this new job in the cluster through the sparklauncher API
                    new SparkLauncherAPI(currentJob).start();
                }
                else {
                    //No resources was assigned for this job. Do nothing and the job will remain on the top of the queue
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Log.SchedulerLogging.log(Level.SEVERE,RoundRobinScheduler.class.getName()+e.toString());
                    }
                }
            }
            else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Log.SchedulerLogging.log(Level.SEVERE,RoundRobinScheduler.class.getName()+e.toString());
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
                SchedulerUtil.agentList.get(i).setAlive(true);

                // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole,currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Unreserved CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" from the default scheduler-role");
                // use http api reserve-method to reserve resources in this agent
                HTTPAPI.RESERVE(currentJob.getRole(),currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()),SchedulerUtil.agentList.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO,RoundRobinScheduler.class.getName()+" Reserved CPU: "+currentJob.getCoresPerExecutor()+" Mem: "+Math.ceil(currentJob.getTotalExecutorMemory())+" to Job: "+currentJob.getJobID()+" with Role: "+currentJob.getRole());
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
