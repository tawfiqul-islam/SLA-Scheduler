package JobMananger;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.*;

public class JobSubmit {

    private static final Logger logger = Logger.getLogger(JobSubmit.class.getName());

    public static JSONObject constructJobRequest(int exec, int cpe, double mpe, String inPath, String outPath, String jarPath, String mainClass, String appArgs)  {

        JSONObject jsonObj = new JSONObject();
        try {
            jsonObj.put("executors", exec);
            jsonObj.put("coresPerExecutor", cpe);
            jsonObj.put("memoryPerExecutor", mpe);
            jsonObj.put("inputPath", inPath);
            jsonObj.put("outputPath", outPath);
            jsonObj.put("appJarPath", jarPath);
            jsonObj.put("mainClassName", mainClass);
            jsonObj.put("appArgs",appArgs);

        }catch(Exception e) {
            logger.log(Level.SEVERE,"Exception while constructing job request json",e);
        }
        return jsonObj;
    }

    public static void main(String args[])  {
        for(int i=0;i<10;i++) {

            JSONObject jsonObj = constructJobRequest(2, 3, 6.0,
                    "/home/ubuntu/research/data/myinput",
                    "/home/ubuntu/research/data/myoutput",
                    "/home/ubuntu/experiments/bigdatabench-spark_1.3.0-hadoop_1.0.4.jar",
                    "cn.ac.ict.bigdatabench.WordCount",
                            "");
            try {
                Socket clientSocket = new Socket("115.146.92.100",9066);
                DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                outToServer.writeBytes(jsonObj.toString());
                clientSocket.close();
                System.out.println(jsonObj);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Exception while sending Job Request to Scheduler", e);
            }
        }
    }
}
