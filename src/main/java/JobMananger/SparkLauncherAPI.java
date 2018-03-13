package JobMananger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import Entity.Job;
import Scheduler.SchedulerUtil;
import org.apache.spark.launcher.SparkLauncher;
import java.util.logging.Level;
import Settings.Settings;

public class SparkLauncherAPI extends Thread{

    Job jobObj;

    public SparkLauncherAPI(Job jobObj) {
        this.jobObj = jobObj;
    }

    public void run() {

        Process pr = null;
        try {
            //System.out.println(jobObj);
            SparkLauncher sl = new SparkLauncher();
            sl.setSparkHome(Settings.sparkHome);
            sl.setMaster(Settings.mesosMasterSpark);
            sl.setMainClass(jobObj.getMainClassName());
            sl.setConf("spark.executor.cores", Integer.toString(jobObj.getCoresPerExecutor()));
            int execMem = (int)(jobObj.getMemPerExecutor()/1024.0);
            sl.setConf("spark.executor.memory", Integer.toString(execMem)+"g");
            sl.setConf("spark.driver.maxResultSize","3g");
            sl.setConf("spark.driver.memory","3g");
            sl.setConf("spark.cores.max", Integer.toString(jobObj.getCoresPerExecutor()*jobObj.getExecutors()));
            sl.setConf("spark.mesos.role",jobObj.getRole());
            sl.setAppResource(jobObj.getAppJarPath());
            sl.addAppArgs(jobObj.getInputPath());
            if(jobObj.getAppArgs().length()>0)
                sl.addAppArgs(jobObj.getAppArgs());
            sl.addAppArgs(jobObj.getOutputPath());

            pr = sl.launch();

        } catch (IOException e) {
            Log.SchedulerLogging.log(Level.SEVERE,SparkLauncherAPI.class.getName()+" Exception in sparkLauncher construction: "+ e.toString());
        }

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(pr.getInputStream(),jobObj.getJobID());
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(pr.getErrorStream(),jobObj.getJobID());
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();
        try {
            pr.waitFor();
        } catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,SparkLauncherAPI.class.getName()+" Exception in sparkLauncher waiting process: "+ e.toString());
        }
    }

    public class InputStreamReaderRunnable implements Runnable {

        private BufferedReader reader;

        private String jobID;
        public InputStreamReaderRunnable(InputStream is, String jobID) {
            this.reader = new BufferedReader(new InputStreamReader(is));
            this.jobID = jobID;
        }

        public void run() {

            FileOutputStream fos = null;
            File file;
            String contents="";
            try {
                String line = reader.readLine();
                int i=0;
                while (line != null) {
                    contents+=(i++)+": "+line+"\n";
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                file = new File(SchedulerUtil.schedulerHome+"/logs/spark-app-"+jobID+".txt");
                fos = new FileOutputStream(file);

                if (!file.exists()) {
                    file.createNewFile();
                }

                byte[] bytesArray = contents.getBytes();

                fos.write(bytesArray);
                fos.flush();
                //System.out.println("File Written Successfully");
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
            finally {
                try {
                    if (fos != null) {
                        fos.close();
                    }
                }
                catch (IOException ioe) {
                    System.out.println("Error in closing the Stream");
                }
            }
        }
    }

}
