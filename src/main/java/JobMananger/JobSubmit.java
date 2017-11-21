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
        for(int i=0;i<3;i++) {

            JSONObject jsonObj = constructJobRequest(1, 1, 1.0,
                    "/home/tawfiq/research/spark/myinput",
                    "/home/tawfiq/research/spark/myoutput",
                    "/home/tawfiq/research/spark/bigdatabench-spark_1.3.0-hadoop_1.0.4.jar",
                    "application.class=cn.ac.ict.bigdatabench.WordCount",
                            "");
            try {
                Socket clientSocket = new Socket("127.0.0.1",9066);
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
