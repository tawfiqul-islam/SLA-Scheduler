package Settings;

import Scheduler.SchedulerUtil;

import java.util.Properties;
import java.lang.*;
import java.io.*;
import java.util.logging.Level;

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
		input = new FileInputStream("profiler.ini");

		// load a properties file
		prop.load(input);

        SchedulerUtil.schedulerRole=prop.getProperty("scheduler.role");
        SchedulerUtil.schedulerHome=prop.getProperty("scheduler.home");
        SchedulerUtil.schedulerAlgorithm=Integer.parseInt(prop.getProperty("scheduler.algorithm"));
        SchedulerUtil.schedulerIP=prop.getProperty("scheduler.ip");
        SchedulerUtil.jobHandlerPort=Integer.parseInt(prop.getProperty("scheduler.port"));
        SchedulerUtil.schedulingInterval=Long.parseLong(prop.getProperty("schedulingInterval"));
        SchedulerUtil.resourceSplitThreshold=Double.parseDouble(prop.getProperty("unificationThreshold"));
		Settings.sparkHome=prop.getProperty("spark.home");
		Settings.mesosMasterURI=prop.getProperty("mesos.masterURI");
		Settings.mesosMasterSpark=prop.getProperty("mesos.masterURIspark");

	} catch (IOException ex) {
        Log.SchedulerLogging.log(Level.SEVERE,SettingsLoader.class.getName()+" Exception while loading settings: "+ex);
	} finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
                Log.SchedulerLogging.log(Level.SEVERE,SettingsLoader.class.getName()+" Exception while closing inputstream: "+e);
			}
		}
	}
  }
}