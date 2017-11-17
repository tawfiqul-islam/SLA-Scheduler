package Settings;

import Scheduler.SchedulerUtil;

import java.util.Properties;
import java.lang.*;
import java.io.*;

/*
 * Loads the settings from the profiler configuration file
 * 
 * @author: Muhammed Tawfiqul Islam
 * 
 */

public class SettingsLoader {
  public static void loadSettings() {

	Properties prop = new Properties();
	InputStream input = null;

	try {

		//specify properties file with full path... or only file name if it's in current directory
		input = new FileInputStream("src/main/resources/profiler.ini");

		// load a properties file
		prop.load(input);

        SchedulerUtil.schedulerAlgorithm=Integer.parseInt(prop.getProperty("scheduler.algorithm"));
        SchedulerUtil.schedulerIP=prop.getProperty("scheduler.ip");
        SchedulerUtil.jobHandlerPort=Integer.parseInt(prop.getProperty("scheduler.port"));
		Settings.sparkHome=prop.getProperty("spark.home");
		Settings.mesosMasterURI=prop.getProperty("mesos.masterURI");

	} catch (IOException ex) {
		ex.printStackTrace();
	} finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

  }
}