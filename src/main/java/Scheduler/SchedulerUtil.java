package Scheduler;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

public class SchedulerUtil {

    private static final Logger logger = Logger.getLogger(SchedulerUtil.class.getName());
    public static int jobHandlerPort;
    public static String schedulerIP;
    public static int schedulerAlgorithm;
    public static Queue<Job>  jobQueue = new LinkedList<Job>();
    public static ArrayList<Job> finishedJobList = new ArrayList<Job>();

    public static synchronized Job queueOperation(Job obj, boolean addOperation) {
        if (addOperation) {
            jobQueue.add(obj);
            logger.info("New Job Added to job queue with id: " + obj.getJobID());
            return null;
        }
        else{
            Job removedJob = jobQueue.remove();
            logger.info("Job "+removedJob.getJobID()+" removed from job queue and added to job finished List");
            return removedJob;
        }
    }

    public static void printJobQueue() {
        for(Object object : jobQueue) {
            Job jobObj = (Job) object;
            System.out.println(jobObj.toString());
        }
    }
}
