package Operator;

import Cluster.Agent;

import java.util.ArrayList;

public class OperatorUtil {

    public static ArrayList<Agent> agentList= new ArrayList<Agent>();

    public static void printAgentList()
    {
        System.out.println("Printing Agent Details: ");
        for(int i=0;i<agentList.size();i++)
            System.out.println("Agent #"+(i+1)+": "+agentList.get(i).toString());
    }
}
