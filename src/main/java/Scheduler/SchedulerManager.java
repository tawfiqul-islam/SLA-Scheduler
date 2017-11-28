package Scheduler;


import java.util.logging.Level;
import JobMananger.JobRequestHandler;
import Operator.HTTPAPI;
import Settings.SettingsLoader;
import java.util.logging.Level;

public class SchedulerManager {

    public static void main(String args[]) {
        //new SchedulerManager();
        //load Settings
        SettingsLoader.loadSettings();
        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Loaded Settings from Configuration File");

        //check cluster health...fails then throw exception/error or wait for cluster to be ready
        while(true)
        {
            if(HTTPAPI.GET_HEALTH()) {
                Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Cluster is running and healthy");
                break;
            }
            else {
                Log.SchedulerLogging.log(Level.SEVERE,SchedulerManager.class.getName()+": Cluster has not started");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //Get cluster status: agentlist
        SchedulerUtil.agentList=HTTPAPI.GET_AGENT();
        //SchedulerUtil.printAgentList();
        //allocate all resources to the scheduler-role:
        //when some resources need to be allocated for a job, unreserve it first from scheduler-role then reserve it for that job's role
        //after job completion, the resources needs to be unreserved from the job and then reserved back again to the scheduler-role

        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Reserving all the cluster resources to the default scheduler-role");
        for(int i=0;i<SchedulerUtil.agentList.size();i++)
        {
            HTTPAPI.RESERVE(SchedulerUtil.schedulerRole,SchedulerUtil.agentList.get(i).getCpu(),SchedulerUtil.agentList.get(i).getMem(),SchedulerUtil.agentList.get(i).getId());
        }

        //start scheduler
        if(SchedulerUtil.schedulerAlgorithm==Algorithm.RoundRobin) {
            RoundRobinScheduler rrSchedulerObj = new RoundRobinScheduler();
            rrSchedulerObj.start();
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started RoundRobin Scheduler");
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.BFHeuristic) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started BFHeuristic Scheduler");
        }
        else {
            //log error..not scheduling algorithm is chosen...or use a defautl scheduler
            Log.SchedulerLogging.log(Level.SEVERE,SchedulerManager.class.getName()+": No Scheduler Algorithm was selected in configuration");
        }
        //start job handler
        JobRequestHandler jobRequestHandlerObj = new JobRequestHandler();
        jobRequestHandlerObj.start();
        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started JobRequestHandler");
    }
}


