package Scheduler;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
import org.apache.spark.launcher.SparkLauncher;

import Settings.Settings;

public class JobSubmitter {

     void submit(String appClass,String cores, String memory, String coresMax, String mesosCons, String appJarPath, String inputPath, String outputPath, String appArgs) {
        Process pr = null;
        try {

            SparkLauncher sl = new SparkLauncher();
            sl.setSparkHome(Settings.sparkHome);
            sl.setMaster(Settings.sparkMaster);
            sl.setMainClass(appClass);
            sl.setConf("spark.executor.cores", cores);
            sl.setConf("spark.executor.memory", memory);
            sl.setConf("spark.cores.max", coresMax);
            sl.setConf("spark.mesos.constraints",mesosCons);
            sl.setAppResource(appJarPath);
            sl.addAppArgs(inputPath);
            if(appArgs!=null)
                sl.addAppArgs(appArgs);
            sl.addAppArgs(outputPath + "/" + UUID.randomUUID().toString());

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
