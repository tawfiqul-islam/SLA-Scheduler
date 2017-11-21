package Scheduler;

import JobMananger.JobRequestHandler;
import Operator.HTTPAPI;
import Settings.SettingsLoader;

public class SchedulerManager {

    //Load Settings for Profiler
    public static void main(String args[]) {
        //load Settings
        SettingsLoader.loadSettings();

        //check cluster health...fails then throw exception/error or wait for cluster to be ready
        while(true)
        {
            if(HTTPAPI.GET_HEALTH()) {
                //log cluster has started
                break;
            }
            else {
                //log cluster is not ready yet
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //Get cluster status: agentlist
        SchedulerUtil.agentList=HTTPAPI.GET_AGENT();
        SchedulerUtil.printAgentList();
        //start scheduler
        if(SchedulerUtil.schedulerAlgorithm==Algorithm.RoundRobin) {
            RoundRobinScheduler rrSchedulerObj = new RoundRobinScheduler();
            rrSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.BFHeuristic) {

        }
        else {
            //log error..not scheduling algorithm is chosen...or use a defautl scheduler
        }
        //start job handler
        JobRequestHandler jobRequestHandlerObj = new JobRequestHandler();
        jobRequestHandlerObj.start();
    }
      /*


        SparkLauncherAPI SparkLauncherAPIobj = new SparkLauncherAPI();

        SparkLauncherAPIobj.submit("cn.ac.ict.bigdatabench.Sort","3","2g",
                "6","id:slave1",
                "/home/tawfiq/sp/spark-2.0.1/bigdatabench-spark_1.3.0-hadoop_1.0.4.jar",
                "/home/tawfiq/sp/spark-2.0.1/myinput",
                "/home/tawfiq/sp/spark-2.0.1/myinput /home/tawfiq/sp/spark-2.0.1/myoutput",
                null);

    }*/
}
