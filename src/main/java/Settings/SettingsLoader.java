package Settings;

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


		Settings.workerNumbers=Integer.parseInt(prop.getProperty("worker.numbers"));
		Settings.workerCores=Integer.parseInt(prop.getProperty("worker.cores"));
		Settings.workerMemory=Integer.parseInt(prop.getProperty("worker.memory"));
		Settings.executorCoresLimit=Integer.parseInt(prop.getProperty("executor.cores.limit"));
		Settings.profilerInputSize=Integer.parseInt(prop.getProperty("profiler.input.size"));
		Settings.reProfile=Integer.parseInt(prop.getProperty("reprofile.size"));
		Settings.repeatConfig=Integer.parseInt(prop.getProperty("repeat.config"));
		Settings.profilerLevel=prop.getProperty("profiler.level");
		Settings.sparkHome=prop.getProperty("spark.home");
		Settings.inputPathProfiler=prop.getProperty("profiler.input.path");
		Settings.inputPathApplication=prop.getProperty("application.input.path");
		Settings.applicationJar=prop.getProperty("application.jar.path");
		Settings.applicationClass=prop.getProperty("application.class");
		Settings.outputPath=prop.getProperty("application.outputPath");
		Settings.sparkMaster=prop.getProperty("spark.master");
		Settings.appArgs=prop.getProperty("appArgs");
		Settings.coreCost=Double.parseDouble(prop.getProperty("coreCost"));
		
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