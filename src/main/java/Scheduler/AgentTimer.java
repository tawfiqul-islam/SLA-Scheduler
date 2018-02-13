package Scheduler;

import java.util.TimerTask;

public class AgentTimer extends TimerTask {

    public void run() {

        for(int i=0;i<SchedulerUtil.agentList.size();i++)
        {
            if(SchedulerUtil.agentList.get(i).isUsed())
            {
                SchedulerUtil.agentList.get(i).setRunTime(SchedulerUtil.agentList.get(i).getRunTime()+1);
            }
        }
    }
}