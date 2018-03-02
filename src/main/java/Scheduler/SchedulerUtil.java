package Scheduler;

import Entity.Agent;
import Entity.Job;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.*;
import java.util.logging.Level;

public class SchedulerUtil {

    public static String schedulerHome;
    public static String schedulerRole;
    public static int jobHandlerPort;
    public static String schedulerIP;
    public static int schedulerAlgorithm;
    public static double resourceSplitThreshold=0.25;
    public static List<Job> jobQueue = Collections.synchronizedList(new ArrayList<Job>());
    public static List<Job> fullySubmittedJobList = Collections.synchronizedList(new ArrayList<Job>());
    public static List<Job> finishedJobList = Collections.synchronizedList(new ArrayList<Job>());
    public static List<Agent> agentList;

    public static void printJobQueue() {
        for (int i=0;i<jobQueue.size();i++) {
            System.out.println(jobQueue.get(i).toString());
        }
    }
    public static void printAgentList()
    {
        System.out.println("Printing Agent Details: ");
        for(int i=0;i<agentList.size();i++)
            System.out.println("Agent #"+(i+1)+": "+agentList.get(i).toString());
    }
    public static void resourceReservation(ArrayList<Agent> placedAgents, Job currentJob, Class classVar)
    {
        for(int i=0;i<placedAgents.size();i++) {

            boolean httpOperation = false;
            while (true) {
                // use http api unreserve-method to first unreserve the resources from the default scheduler-role
                ServerResponse resObj = HTTPAPI.UNRESERVE(SchedulerUtil.schedulerRole, currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), placedAgents.get(i).getId());
                Log.SchedulerLogging.log(Level.INFO, classVar.getName() + " Trying to UnReserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " from the default scheduler-role" + " in agent " + placedAgents.get(i).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());
                // use http api reserve-method to reserve resources in this agent
                if (resObj.getStatusCode() != 409) {

                    while (true) {

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        resObj = HTTPAPI.RESERVE(currentJob.getRole(), currentJob.getCoresPerExecutor(), Math.ceil(currentJob.getTotalExecutorMemory()), placedAgents.get(i).getId());
                        Log.SchedulerLogging.log(Level.INFO, classVar.getName() + " Trying to Reserve CPU: " + currentJob.getCoresPerExecutor() + " Mem: " + Math.ceil(currentJob.getTotalExecutorMemory()) + " to Job: " + currentJob.getJobID() + " with Role: " + currentJob.getRole() + " in agent " + placedAgents.get(i).getId() + " ServerResponse: " + resObj.getResponseString() + " Status Code: " + resObj.getStatusCode());

                        if (resObj.getStatusCode() != 409) {
                            Log.SchedulerLogging.log(Level.INFO, classVar.getName() + "*RESERVATION SUCCESSFUL* --> Current Status of Agent: " + placedAgents.get(i).getId() + "-> CPU: " + placedAgents.get(i).getCpu() + " Mem: " + placedAgents.get(i).getMem());
                            //update the available resources in this agent
                            placedAgents.get(i).setUsed(true);
                            //add agent Id to the job
                            currentJob.getAgentList().add(placedAgents.get(i).getId());
                            httpOperation = true;
                            break;
                        }

                    }
                }
                if (httpOperation) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}