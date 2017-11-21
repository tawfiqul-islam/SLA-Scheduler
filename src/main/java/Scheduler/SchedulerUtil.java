package Scheduler;

import Entity.Agent;
import Entity.Job;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

public class SchedulerUtil {

    private static final Logger logger = Logger.getLogger(SchedulerUtil.class.getName());
    public static int jobHandlerPort=9066;
    public static String schedulerIP="127.0.0.1";
    public static int schedulerAlgorithm;
    public static Queue<Job> newJobQueue = new LinkedList<Job>();
    public static ArrayList<Job> partialSubmittedJobList = new ArrayList<>();
    public static ArrayList<Job> fullySubmittedJobList = new ArrayList<>();
    public static ArrayList<Job> finishedJobList = new ArrayList<>();
    public static ArrayList<Agent> agentList;

    public static synchronized Job queueOperation(Job obj, int choice) {

        //add new job in queue
        if(choice==1) {
            newJobQueue.add(obj);
            logger.info("New Job Added to job queue with id: " + obj.getJobID());
            return null;
        }
        //inspect the top job in queue
        else if(choice==2) {
            return newJobQueue.element();
        }
        //remove a job from queue
        else {
            logger.info("Job " + newJobQueue.element().getJobID() + " is removed from job queue");
            return newJobQueue.remove();
        }
    }

    public static void printJobQueue() {
        for (Object object : newJobQueue) {
            Job jobObj = (Job) object;
            System.out.println(jobObj.toString());
        }
    }
    public static void printAgentList()
    {
        System.out.println("Printing Agent Details: ");
        for(int i=0;i<agentList.size();i++)
            System.out.println("Agent #"+(i+1)+": "+agentList.get(i).toString());
    }
}
