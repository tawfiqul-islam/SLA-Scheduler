package Scheduler;

import JobMananger.JobRequestHandler;
import Operator.HTTPAPI;
import Settings.SettingsLoader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.logging.Level;

public class SchedulerManager {

    private static PrintWriter pw;
    static long startTime;

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

        //start job handler
        JobRequestHandler jobRequestHandlerObj = new JobRequestHandler();
        jobRequestHandlerObj.start();
        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started JobRequestHandler");

        //open output file for writing results
        try {
            StatusUpdater.sb.append("id");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("arrival-time");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("start-time");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("finish-time");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("scheduling-delay");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("duration");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("E");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("E-allocated");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("CPE");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("MPE");
            StatusUpdater.sb.append(',');
            StatusUpdater.sb.append("Agents");
            StatusUpdater.sb.append('\n');

            pw = new PrintWriter(new File(SchedulerUtil.schedulerHome+"/output.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //start scheduler
        if(SchedulerUtil.schedulerAlgorithm==Algorithm.RoundRobin) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started RoundRobin Scheduler ");
            startTime=System.currentTimeMillis();
            RoundRobinScheduler rrSchedulerObj = new RoundRobinScheduler();
            rrSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.BFHeuristic) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started BestFit Scheduler ");
            startTime=System.currentTimeMillis();
            BestFitScheduler bfSchedulerObj = new BestFitScheduler();
            bfSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.FirstFit) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started FirstFit Scheduler ");
            startTime=System.currentTimeMillis();
            FirstFitDScheduler ffdSchedulerObj = new FirstFitDScheduler();
            ffdSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.Morpheus) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started Morpheus Scheduler ");
            startTime=System.currentTimeMillis();
            MorpheusScheduler morpheusSchedulerObj = new MorpheusScheduler();
            morpheusSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.BFDeadline) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started BestFit Deadline Scheduler ");
            startTime=System.currentTimeMillis();
            BestFitDeadlineScheduler bfdSchedulerObj = new BestFitDeadlineScheduler();
            bfdSchedulerObj.start();
        }
        else if(SchedulerUtil.schedulerAlgorithm==Algorithm.FFDeadline) {
            Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started FirstFit Deadline Scheduler ");
            startTime=System.currentTimeMillis();
            FirstFitDeadlineScheduler ffdSchedulerObj = new FirstFitDeadlineScheduler();
            ffdSchedulerObj.start();
        }
        else {
            //log error..not scheduling algorithm is chosen...or use a default scheduler
            Log.SchedulerLogging.log(Level.SEVERE,SchedulerManager.class.getName()+": No Scheduler Algorithm was selected in configuration");
        }

        //start job updater
        //Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Started job updater ");

        //StatusUpdater StatusUpdaterObj = new StatusUpdater();
        //StatusUpdaterObj.start();

        //start agent timer
        Timer timer = new Timer();
        timer.schedule(new AgentTimer(), 0, 1000);

    }
    public static void shutDown()
    {
        //finish writing results in output.csv file
        pw.write(StatusUpdater.sb.toString());
        pw.close();
        //writing per agent execution time in agent-times.csv file
        writeAgentResults();
        //writing per second cost and cumulative cost in cost.csv file
        writeCostResults();

        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": Finished writing results for scheduler");
        Log.SchedulerLogging.log(Level.INFO,SchedulerManager.class.getName()+": ***Shutting down SchedulerManager*** ==>Total MakeSpan: "+(System.currentTimeMillis()-startTime)/1000+" seconds.");
        System.exit(0);
    }

    public static void writeCostResults()
    {
        try {
            pw = new PrintWriter(new File(SchedulerUtil.schedulerHome+"/cost.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("time");
        sb.append(',');
        sb.append("cost");
        sb.append(',');
        sb.append("cumulative_cost");
        sb.append(',');
        sb.append("costPerMin");
        sb.append('\n');

        for(int i=0;i< AgentTimer.costList.size();i++) {
            sb.append(i+1);
            sb.append(',');
            sb.append(AgentTimer.costList.get(i));
            sb.append(',');
            sb.append(AgentTimer.cumulativeCostList.get(i));

            if(i<AgentTimer.cumulativeCostPerMinList.size()) {
                sb.append(',');
                sb.append(AgentTimer.cumulativeCostPerMinList.get(i));
            }

            sb.append('\n');
        }
        pw.write(sb.toString());
        pw.close();
    }

    public static void writeAgentResults()
    {
        try {
            pw = new PrintWriter(new File(SchedulerUtil.schedulerHome+"/agent-times.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("id");
        sb.append(',');
        sb.append("cpu");
        sb.append(',');
        sb.append("mem");
        sb.append(',');
        sb.append("execution-time");
        sb.append('\n');

        for(int i=0;i< SchedulerUtil.agentList.size();i++) {
            sb.append(SchedulerUtil.agentList.get(i).getId());
            sb.append(',');
            sb.append(SchedulerUtil.agentList.get(i).getDefaultCPU());
            sb.append(',');
            sb.append(SchedulerUtil.agentList.get(i).getDefaultMEM());
            sb.append(',');
            sb.append(SchedulerUtil.agentList.get(i).getRunTime());
            sb.append('\n');

        }
        pw.write(sb.toString());
        pw.close();
    }
}


