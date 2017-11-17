package Scheduler;
import java.util.logging.Logger;
import java.util.HashMap;

public class SchedulerUtil {

    private static final Logger logger = Logger.getLogger(SchedulerUtil.class.getName());
    public static int jobHandlerPort;
    public static String schedulerIP;
    public static int schedulerAlgorithm;

    public static HashMap<String,Job> jobQueue = new HashMap<String, Job>();
    public static HashMap<String,Job> finishedJobList = new HashMap<String, Job>();

    public static synchronized void queueOperation(String id, Job obj, boolean addOperation) {
        if (addOperation) {
            jobQueue.put(id, obj);
            logger.info("New Job Added to job queue with id: " + id);
        }
        else{
            finishedJobList.put(id, jobQueue.get(id));
            jobQueue.remove(id);
            logger.info("Job "+id+" removed from job queue and added to job finished queue");
        }
    }

    public static void printJobQueue()
    {
        for (String key : jobQueue.keySet()) {
            Job jobobj = jobQueue.get(key);
            System.out.println(jobobj.toString());
        }
    }
}
