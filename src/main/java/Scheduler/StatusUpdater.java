package Scheduler;

import Entity.Agent;
import Entity.Framework;
import Entity.Job;
import Operator.HTTPAPI;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;

public class StatusUpdater extends Thread{

    //update agents....if new agents are found add them to agent list. if agent is unreachable/inactive then delete them from agent list

    static StringBuilder sb = new StringBuilder();

    static boolean end=false;
    public static void updateAgents()
    {
        ArrayList<Agent> updatedAgentList;
        updatedAgentList=HTTPAPI.GET_AGENT();

        //delete unlisted/inactive agents
        for(int k=0;k<SchedulerUtil.agentList.size();k++)
        {
            boolean present=false;
            for(int m=0;m<updatedAgentList.size();m++)
            {
                if(updatedAgentList.get(m).getId().equalsIgnoreCase(SchedulerUtil.agentList.get(k).getId()))
                {
                    present=true;
                }
            }

            if(!present)
            {
                SchedulerUtil.agentList.remove(k);
                Log.SchedulerLogging.log(Level.INFO,StatusUpdater.class.getName()+" Removed inactive agent with id: "+SchedulerUtil.agentList.get(k).getId()+" from agent list");
                k--;
            }
        }

        //update agent list
        for(int i=0;i<updatedAgentList.size();i++) {
            boolean exists=false;
            for(int j=0;j< SchedulerUtil.agentList.size();j++)
            {
                if(updatedAgentList.get(i).getId().equalsIgnoreCase(SchedulerUtil.agentList.get(j).getId()))
                {
                    exists=true;
                    if(!updatedAgentList.get(i).isActive())
                    {
                        //agent is inactive, delete it from agentlist
                        SchedulerUtil.agentList.remove(j);
                        Log.SchedulerLogging.log(Level.INFO,StatusUpdater.class.getName()+" Removed inactive agent with id: "+SchedulerUtil.agentList.get(j).getId()+" from agent list");
                    }
                }
            }

            if(!exists)
            {
                //new agent found, add it to agentlist
                SchedulerUtil.agentList.add(updatedAgentList.get(i)) ;
                Log.SchedulerLogging.log(Level.INFO,StatusUpdater.class.getName()+" Added a new agent with id: "+updatedAgentList.get(i).getId()+" to the agent list");
            }
        }

        //update total resources in each agent, used for bfhscheduler
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            SchedulerUtil.agentList.get(i).setResourceTotal();
        }
    }

    //update frameworks (jobs)...find newly submitted jobs with matching roles in internal structures and update their framework id
    //check whether any job/framework is finished and is in job/fully submitted list. if so move them to finished queue.
    //unreserve resources for finished frameworks, update agent available resources.

    public static void updateJobs()
    {
        ArrayList<Framework> frameworkList = HTTPAPI.GET_FRAMEWORK();
        for(int i=0;i<frameworkList.size();i++) {
            boolean found = false;
            Job currentJob = null;

            synchronized (SchedulerUtil.jobQueue) {
                synchronized (SchedulerUtil.fullySubmittedJobList) {

                    //try to find the current framework in fullysubmittedjoblist
                    for (int j = 0; j < SchedulerUtil.fullySubmittedJobList.size(); j++) {
                        if (frameworkList.get(i).getRole().equalsIgnoreCase(SchedulerUtil.fullySubmittedJobList.get(j).getRole())) {
                            found = true;
                            currentJob = SchedulerUtil.fullySubmittedJobList.get(j);
                            currentJob.setFrameworkID(frameworkList.get(i).getID());
                            currentJob.setStartTime(frameworkList.get(i).getStartTime());
                            currentJob.setFinishTime(frameworkList.get(i).getFinishTime());
                            currentJob.setAlive(frameworkList.get(i).isActive());

                            //TODO handle failed jobs here

                            //current job is finished, add it to finishedjoblist
                            if (!currentJob.isAlive() && currentJob.getFinishTime() > 0) {
                                //log job finished with id ...
                                //remove from fullySubmittedList
                                SchedulerUtil.fullySubmittedJobList.remove(j);
                                Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from fullySubmittedJobList");
                                //add in finished job list
                                SchedulerUtil.finishedJobList.add(currentJob);
                                Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Added Job: " + currentJob.getJobID() + " to finishedJobList");
                            }
                            break;
                        }
                    }

                    //try to find the current framework in joblist
                    if (!found) {

                        for (int j = 0; j < SchedulerUtil.jobQueue.size(); j++) {
                            if (frameworkList.get(i).getRole().equalsIgnoreCase(SchedulerUtil.jobQueue.get(j).getRole())) {
                                found = true;
                                currentJob = SchedulerUtil.jobQueue.get(j);
                                currentJob.setFrameworkID(frameworkList.get(i).getID());
                                currentJob.setStartTime(frameworkList.get(i).getStartTime());
                                currentJob.setFinishTime(frameworkList.get(i).getFinishTime());
                                currentJob.setAlive(frameworkList.get(i).isActive());

                                //TODO handle failed jobs here

                                //current job is finished, add it to finishedjoblist
                                if (!currentJob.isAlive() && currentJob.getFinishTime() > 0) {
                                    Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Job: " + currentJob.getJobID() + " is finished");
                                    //remove from jobList
                                    SchedulerUtil.jobQueue.remove(j);
                                    Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Removed Job: " + currentJob.getJobID() + " from jobQueue");
                                    //add in finished job list
                                    SchedulerUtil.finishedJobList.add(currentJob);
                                    Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Added Job: " + currentJob.getJobID() + " to finishedJobList");
                                }
                                break;
                            }
                        }

                    }
                }
            }


            if(found)
            {
                //job is finished, and found in either joblist or fullysubmittedlist. Now unreserve resources for this job
                if(!currentJob.isAlive()&&currentJob.getFinishTime()>0) {
                    //unreserve the current job's reserved resources in all the used agents
                    for (int j = 0; j < currentJob.getAgentList().size(); j++) {
                        //unreseving resources in an agent when a job is finished
                        HTTPAPI.UNRESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), currentJob.getAgentList().get(j));
                        Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + " Unreserved CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " from Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole());
                        //give back these resources by reserving to the default scheduler-role
                        HTTPAPI.RESERVE(SchedulerUtil.schedulerRole, currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), currentJob.getAgentList().get(j));
                        Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + " Reserved back CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to the default scheduler-role");

                        for (int k = 0; k < SchedulerUtil.agentList.size(); k++) {
                            if (SchedulerUtil.agentList.get(k).getId().equalsIgnoreCase(currentJob.getAgentList().get(j))) {
                                Agent agentObj = SchedulerUtil.agentList.get(k);
                                agentObj.setCpu(agentObj.getCpu() + currentJob.getCoresPerExecutor());
                                agentObj.setMem(agentObj.getMem() + Math.ceil(currentJob.getTotalExecutorMemory()));
                                //if all the resources of this node are unused, mark it as not used or alive=false
                                //use APIs here to shut down this agent if needed (to optimize VM cost)
                                if (agentObj.getCpu() == agentObj.getDefaultCPU() && agentObj.getMem() == agentObj.getDefaultMEM()) {
                                    agentObj.setUsed(false);
                                }
                                Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + " Current Status of Agent: " + SchedulerUtil.agentList.get(k).getId() + "-> CPU: " + SchedulerUtil.agentList.get(k).getCpu() + " Mem: " + SchedulerUtil.agentList.get(k).getMem());
                                //updating agent resources
                                SchedulerUtil.agentList.set(k, agentObj);
                                break;
                            }
                            //TODO add exception if the agent is not found on agentlist
                        }
                    }

                    //save this job's scheduling details in result str
                    saveCompletedJobDetails(currentJob);
                }
                else {
                    //job is active
                    Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + ": Job: " + currentJob.getJobID() + " is still active");

                }
            }
            //the current framework was not found in any list
            else
            {
                //already added to finish list
                //log error
                //maybe the framework was launched by any past scheduler?
                //Log.SchedulerLogging.log(Level.SEVERE,StatusUpdater.class.getName()+"Framework with id: "+frameworkList.get(i).getID()+"with role: "+frameworkList.get(i).getRole()+" was not found! Launched by any previous scheduler?");
                continue;
            }
        }
    }

    public static void saveCompletedJobDetails(Job currentJob) {

        double arrivalTime=(currentJob.getArrivalTime()-SchedulerManager.startTime)/1000.0;
        double startTime=((currentJob.getStartTime()*0.000001)-SchedulerManager.startTime)/1000.0;
        double finishTime=((currentJob.getFinishTime()*0.000001)-SchedulerManager.startTime)/1000.0;

        //resultID as jobID
        StatusUpdater.sb.append(currentJob.getResultID());
        StatusUpdater.sb.append(',');
        //arrival time
        StatusUpdater.sb.append(arrivalTime);
        StatusUpdater.sb.append(',');
        //job start time
        StatusUpdater.sb.append(startTime); //converting from nano to millis
        StatusUpdater.sb.append(',');
        //job finish time
        StatusUpdater.sb.append(finishTime);
        StatusUpdater.sb.append(',');
        //scheduling delay
        StatusUpdater.sb.append(startTime-arrivalTime);
        StatusUpdater.sb.append(',');
        //job duration
        StatusUpdater.sb.append(finishTime-startTime);
        StatusUpdater.sb.append(',');
        //executors
        StatusUpdater.sb.append(currentJob.getExecutors());
        StatusUpdater.sb.append(',');
        //allocated executors
        StatusUpdater.sb.append(currentJob.getAllocatedExecutors());
        StatusUpdater.sb.append(',');
        //cores
        StatusUpdater.sb.append(currentJob.getCoresPerExecutor());
        StatusUpdater.sb.append(',');
        //memory
        StatusUpdater.sb.append(currentJob.getMemPerExecutor());
        StatusUpdater.sb.append(',');
        //number of distinct agents used
        StatusUpdater.sb.append(currentJob.getAgentList().stream().distinct().count());
        StatusUpdater.sb.append('\n');

    }
    public static String getCurrentTimeStamp(long time) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date(time);
        String strDate = sdfDate.format(now);
        return strDate;
    }
    public void run()
    {
        while(true) {
            updateJobs();
            if(SchedulerManager.ShutDown&&SchedulerUtil.fullySubmittedJobList.size()==0) {
                Log.SchedulerLogging.log(Level.INFO, StatusUpdater.class.getName() + " Shutting down StatusUpdater. Total Job Run-Time: " + (System.currentTimeMillis() - SchedulerManager.startTime) / 1000 + " seconds");
                SchedulerManager.shutDown();
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
