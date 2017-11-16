package Operator;

import Cluster.Agent;

import java.util.ArrayList;

public class Util {

    public static int jobHandlerPort=9066;
    public static String schedulerIP="127.0.0.1";
    public static ArrayList<Agent> agentList= new ArrayList<Agent>();

    public static void printAgentList()
    {
        System.out.println("Printing Agent Details: ");
        for(int i=0;i<agentList.size();i++)
            System.out.println("Agent #"+(i+1)+": "+agentList.get(i).toString());
    }
}
