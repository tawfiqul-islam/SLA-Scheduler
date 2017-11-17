package Scheduler;

import Operator.OperatorUtil;

public class RoundRobinScheduler extends Thread{

    public void run() {
        while(true)
        {
            Job currentJob;
            if(!SchedulerUtil.jobQueue.isEmpty()) {

                currentJob=SchedulerUtil.queueOperation(null,false);

                int executorCount=currentJob.getExecutors();

                boolean reserved=false;
                for(int j=0;j<OperatorUtil.agentList.size();j++) {

                    if(OperatorUtil.agentList.get(j).getCpu()>=currentJob.getCoresPerExecutor() &&
                            OperatorUtil.agentList.get(j).getMem()>=currentJob.getMemPerExecutor()) {
                        OperatorUtil.agentList.get(j).setCpu(OperatorUtil.agentList.get(j).getCpu()-currentJob.getCoresPerExecutor());
                        OperatorUtil.agentList.get(j).setMem(OperatorUtil.agentList.get(j).getMem()-currentJob.getMemPerExecutor());
                        //reserve an executor in this agent
                        //call http api method to reserve
                        executorCount--;
                        reserved=true;
                    }
                    if(executorCount==0) {
                        break;
                    }
                    if(j==OperatorUtil.agentList.size()-1) {
                        j=-1;
                        if(reserved) {
                            reserved = false;
                        }
                        else{
                            break;
                        }
                    }
                }

                if(executorCount==currentJob.getExecutors()) {
                    //no executor created for current job.
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
}
