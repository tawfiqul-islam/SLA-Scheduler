package JobMananger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

import Entity.Job;
import Scheduler.SchedulerUtil;
import org.json.*;
import java.util.UUID;

public class JobProcessor extends Thread {

    InputStream in;
    Socket receiverSocket;
    private static int role = 0;
    public JobProcessor(Socket socket) {
        receiverSocket=socket;
    }
    public static synchronized int getRole() {
        return role++;
    }

    Job parseMessage(String requestStr) {
        Job jobObj = new Job();
        jobObj.setJobID(UUID.randomUUID().toString());
        try {
            JSONObject requestJson = new JSONObject(requestStr);
            jobObj.setExecutors(requestJson.getInt("executors"));
            jobObj.setCoresPerExecutor(requestJson.getInt("coresPerExecutor"));
            jobObj.setMemPerExecutor(requestJson.getDouble("memoryPerExecutor")*1024.0);
            jobObj.setExecutorMemoryOverhead();
            jobObj.setResourceSplit();
            jobObj.setInputPath(requestJson.getString("inputPath"));
            jobObj.setOutputPath(requestJson.getString("outputPath")+"/"+jobObj.getJobID());
            jobObj.setAppJarPath(requestJson.getString("appJarPath"));
            jobObj.setMainClassName(requestJson.getString("mainClassName"));
            jobObj.setAppArgs(requestJson.getString("appArgs"));
            jobObj.setArrivalTime(getCurrentTimeStamp());
            jobObj.setRole("role"+getRole());
        }catch(Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,JobProcessor.class.getName()+" Exception in parseMessage"+e.toString());
        }
        return jobObj;
    }

    public static String getCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

    public String readData() {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        String resultStr=null;
        try {
            while ((length = in.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // StandardCharsets.UTF_8.name() > JDK 7
        try {
            resultStr= result.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return resultStr;
    }

    public void run() {
        try {
            in= receiverSocket.getInputStream();
        } catch (IOException e) {
            Log.SchedulerLogging.log(Level.SEVERE,JobProcessor.class.getName()+" Exception in run method"+e.toString());
        }
        Job jobObj = parseMessage(readData());
        SchedulerUtil.jobQueue.add(jobObj);
        Log.SchedulerLogging.log(Level.INFO,JobProcessor.class.getName()+": New Job Added to job queue with id: " + jobObj.getJobID());
        SchedulerUtil.printJobQueue();
    }
}
