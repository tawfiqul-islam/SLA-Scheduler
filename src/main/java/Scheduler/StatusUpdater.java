package Scheduler;

import Entity.Agent;
import Entity.Framework;
import Entity.Job;
import Operator.HTTPAPI;

import java.util.ArrayList;

public class StatusUpdater extends Thread{

    //update agents....if new agents are found add them to agent list. if agent is unreachable/inactive then delete them from agent list

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
                    if(!updatedAgentList.get(i).isAlive())
                    {
                        //agent is inactive, delete it from agentlist
                        SchedulerUtil.agentList.remove(j);
                    }
                }
            }

            if(!exists)
            {
                //new agent found, add it to agentlist
                SchedulerUtil.agentList.add(updatedAgentList.get(i)) ;
            }
        }
    }

    //update frameworks (jobs)...find newly submitted jobs with matching roles in internal structures and update their framework id
    //check whether any job/framework is finished and is in partial/fully submitted queue. if so move them to finished queue.
    //unreserve resources for finished frameworks, update agent available resources.

    public static void updateJobs()
    {
        ArrayList<Framework> frameworkList = HTTPAPI.GET_FRAMEWORK();
        for(int i=0;i<frameworkList.size();i++)
        {
            boolean found=false;
            Job currentJob = null;

            //try to find the current framework in fullysubmittedjoblist
            for(int j=0;j<SchedulerUtil.fullySubmittedJobList.size();j++)
            {
                if(frameworkList.get(i).getRole().equalsIgnoreCase(SchedulerUtil.fullySubmittedJobList.get(j).getRole()))
                {
                    found=true;
                    currentJob = SchedulerUtil.fullySubmittedJobList.get(j);
                    currentJob.setFrameworkID(frameworkList.get(i).getID());
                    currentJob.setStartTime(frameworkList.get(i).getStartTime());
                    currentJob.setFinishTime(frameworkList.get(i).getFinishTime());
                    currentJob.setAlive(frameworkList.get(i).isActive());

                    //TO DO handle failed jobs
                    if(!currentJob.isAlive()&&currentJob.getFinishTime()>0)
                    {
                        //log job finished with id ...
                        //remove from fullySubmittedList
                        SchedulerUtil.fullySubmittedJobList.remove(j);
                        //add in finished job list
                        SchedulerUtil.finishedJobList.add(currentJob);
                    }
                    break;
                }
            }

            //try to find the current framework in partialsubmittedjoblist
            if(!found)
            {
                for(int j=0;j<SchedulerUtil.partialSubmittedJobList.size();j++)
                {
                    if(frameworkList.get(i).getRole().equalsIgnoreCase(SchedulerUtil.partialSubmittedJobList.get(j).getRole()))
                    {
                        found=true;
                        currentJob = SchedulerUtil.partialSubmittedJobList.get(j);
                        currentJob.setFrameworkID(frameworkList.get(i).getID());
                        currentJob.setStartTime(frameworkList.get(i).getStartTime());
                        currentJob.setFinishTime(frameworkList.get(i).getFinishTime());
                        currentJob.setAlive(frameworkList.get(i).isActive());

                        //TO DO handle failed jobs
                        if(!currentJob.isAlive()&&currentJob.getFinishTime()>0)
                        {
                            //log job finished with id ...
                            //remove from fullySubmittedList
                            SchedulerUtil.partialSubmittedJobList.remove(j);
                            //add in finished job list
                            SchedulerUtil.finishedJobList.add(currentJob);
                        }
                        break;
                    }
                }
            }

            //the current framework was not found in any list
            if(!found)
            {
                //log error
                //maybe the framework was launched by any past scheduler?
                continue;
            }
            else
            {
                //unreserve the current job's reserved resources in all the used agents
                for(int j=0;j<currentJob.getAgentList().size();j++)
                {
                    //unreseving resources in an agent when a job is finished
                    HTTPAPI.UNRESERVE(currentJob.getRole(),currentJob.getCoresPerExecutor(),currentJob.getMemPerExecutor(),currentJob.getAgentList().get(j));

                    for(int k=0;k<SchedulerUtil.agentList.size();k++)
                    {
                        if(SchedulerUtil.agentList.get(k).getId().equalsIgnoreCase(currentJob.getAgentList().get(j)))
                        {
                            Agent agentObj = SchedulerUtil.agentList.get(k);
                            agentObj.setCpu(agentObj.getCpu()+currentJob.getCoresPerExecutor());
                            agentObj.setMem(agentObj.getMem()+currentJob.getMemPerExecutor());

                            //updating agent resources
                            SchedulerUtil.agentList.set(k,agentObj);
                            break;
                        }
                    }
                }
            }
        }
    }

    public void run()
    {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
