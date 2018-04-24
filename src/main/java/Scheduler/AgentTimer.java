package Scheduler;

import java.util.ArrayList;
import java.util.TimerTask;

public class AgentTimer extends TimerTask {

    public static double cumulativeCostVar=0;
    public static ArrayList<Double> costList = new ArrayList<>();
    public static ArrayList<Double> cumulativeCostList = new ArrayList<>();
    public static ArrayList<Double> cumulativeCostPerMinList = new ArrayList<>();

    public void run() {

        double tempCost=0;
        for(int i=0;i<SchedulerUtil.agentList.size();i++)
        {
            if(SchedulerUtil.agentList.get(i).isUsed())
            {
                SchedulerUtil.agentList.get(i).setRunTime(SchedulerUtil.agentList.get(i).getRunTime()+1);
                tempCost+=(SchedulerUtil.agentList.get(i).getDefaultCPU()/4)*0.24;
            }
        }
        cumulativeCostVar+=tempCost;
        costList.add(tempCost);
        cumulativeCostList.add(cumulativeCostVar);

        if(costList.size()%60==0) {
            cumulativeCostPerMinList.add(cumulativeCostVar);
        }
    }
}