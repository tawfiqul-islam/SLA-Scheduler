package Scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.spark.launcher.SparkLauncher;

import Settings.Settings;

public class SparkLauncherAPI {

     void submit(Job jobObj) {
        Process pr = null;
        try {

            SparkLauncher sl = new SparkLauncher();
            sl.setSparkHome(Settings.sparkHome);
            sl.setMaster(Settings.mesosMasterURI);
            sl.setMainClass(jobObj.getMainClassName());
            sl.setConf("spark.executor.cores", Integer.toString(jobObj.getCoresPerExecutor()));
            sl.setConf("spark.executor.memory", Double.toString(jobObj.getMemPerExecutor()));
            sl.setConf("spark.cores.max", Integer.toString(jobObj.getCoresPerExecutor()*jobObj.getExecutors()));
            sl.setConf("spark.mesos.constraints",jobObj.getRole());
            sl.setAppResource(jobObj.getAppJarPath());
            sl.addAppArgs(jobObj.getInputPath());
            if(jobObj.getAppArgs().length()>0)
                sl.addAppArgs(jobObj.getAppArgs());
            sl.addAppArgs(jobObj.getOutputPath());

            pr = sl.launch();

        } catch (IOException e1) {
            e1.printStackTrace();
        }

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(pr.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(pr.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();
        try {
            pr.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class InputStreamReaderRunnable implements Runnable {

        private BufferedReader reader;

        private String name;

        public InputStreamReaderRunnable(InputStream is, String name) {
            this.reader = new BufferedReader(new InputStreamReader(is));
            this.name = name;
        }

        public void run() {
            System.out.println("InputStream Name:" + name + ":");
            try {
                String line = reader.readLine();
                int i=0;
                while (line != null) {
                    System.out.println((i++)+": "+line);
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
