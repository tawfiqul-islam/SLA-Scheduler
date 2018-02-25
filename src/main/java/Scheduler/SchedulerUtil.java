package Scheduler;

import Entity.Agent;
import Entity.Job;

import java.util.*;

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
}