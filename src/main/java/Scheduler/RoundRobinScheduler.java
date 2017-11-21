package Scheduler;

import Entity.Job;
import JobMananger.SparkLauncherAPI;
import Operator.HTTPAPI;

import java.util.ArrayList;

public class RoundRobinScheduler extends Thread{

    public void run() {

        while(true)
        {
            //update agents, update jobs
            StatusUpdater.updateAgents();
            StatusUpdater.updateJobs();
            Job currentJob;
            //allocate more resources to partial submitted jobs
            for(int i=0;i<SchedulerUtil.partialSubmittedJobList.size();i++)
            {
                currentJob=SchedulerUtil.partialSubmittedJobList.get(i);
                placeExecutor(currentJob);
                if(currentJob.getAllocatedExecutors()==currentJob.getExecutors())
                {
                    //remove job from job queue
                    SchedulerUtil.partialSubmittedJobList.remove(i);
                    //add job to fully submitted job list
                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                    i--;
                }
            }

            //schedule new jobs
            if(!SchedulerUtil.newJobQueue.isEmpty()) {

                currentJob=SchedulerUtil.queueOperation(null,2);
                placeExecutor(currentJob);
                //job submitted with all the required resource
                if(currentJob.getAllocatedExecutors()==currentJob.getExecutors())
                {
                    //remove job from job queue
                    SchedulerUtil.queueOperation(null,3);
                    //add job to fully submitted job list
                    SchedulerUtil.fullySubmittedJobList.add(currentJob);
                    //launch this new job in the cluster through the sparklauncher API
                    new SparkLauncherAPI().submit(currentJob);
                }
                //job submitted with partial resources
                else if(currentJob.getAllocatedExecutors()>0)
                {
                    //remove job from job queue
                    SchedulerUtil.queueOperation(null,3);
                    //add job to partial submitted job list
                    SchedulerUtil.partialSubmittedJobList.add(currentJob);
                    //launch this new job in the cluster through the sparklauncher API
                    new SparkLauncherAPI().submit(currentJob);
                }
                else {
                    //No resources was assigned for this job. Do nothing and the job will remain on the top of the queue
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void placeExecutor(Job currentJob)
    {
        int executorCount=0;

        boolean reserved=false;
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            if(SchedulerUtil.agentList.get(i).getCpu()>=currentJob.getCoresPerExecutor() &&
                    SchedulerUtil.agentList.get(i).getMem()>=currentJob.getMemPerExecutor()) {

                //update the available resources in this agent
                SchedulerUtil.agentList.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu()-currentJob.getCoresPerExecutor());
                SchedulerUtil.agentList.get(i).setMem(SchedulerUtil.agentList.get(i).getMem()-currentJob.getMemPerExecutor());

                // call http api method to reserve resources in this agent
                HTTPAPI.RESERVE(currentJob.getRole(),currentJob.getCoresPerExecutor(), currentJob.getMemPerExecutor(),SchedulerUtil.agentList.get(i).getId());
                //add agent Id to the job
                ArrayList<String> temp = currentJob.getAgentList();
                temp.add(SchedulerUtil.agentList.get(i).getId());
                currentJob.setAgentList(temp);

                executorCount++;
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
    }
}
