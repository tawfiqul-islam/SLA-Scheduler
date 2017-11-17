package Scheduler;

import Settings.Settings;
import Settings.SettingsLoader;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class SchedulerManager {

    //Load Settings for Profiler
    public static void main(String args[])
    {
        SettingsLoader.loadSettings();
        JobRequestHandler jobRequestHandlerObj = new JobRequestHandler();
        jobRequestHandlerObj.start();
    }
      /*


        SparkLauncherAPI SparkLauncherAPIobj = new SparkLauncherAPI();

        SparkLauncherAPIobj.submit("cn.ac.ict.bigdatabench.Sort","3","2g",
                "6","id:slave1",
                "/home/tawfiq/sp/spark-2.0.1/bigdatabench-spark_1.3.0-hadoop_1.0.4.jar",
                "/home/tawfiq/sp/spark-2.0.1/myinput",
                "/home/tawfiq/sp/spark-2.0.1/myinput /home/tawfiq/sp/spark-2.0.1/myoutput",
                null);

    }*/

}
