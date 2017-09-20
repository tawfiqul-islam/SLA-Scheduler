package Profiler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import Settings.Settings;
import org.apache.spark.launcher.SparkLauncher;

/*
 * Deploys the profiler... actually it launches the application using each generated configs,
 * uses only the part of input user specified for profiling only
 * 
 */

public class ProfilerDeployer {

	public static int outputIndex=0;
	void runCommand(String cores, String memory,  String coresMax, int outputIndex)
	{
		Process pr = null;
		try {
			if(Settings.applicationClass.equalsIgnoreCase("cn.ac.ict.bigdatabench.Grep"))
			{
					pr = new SparkLauncher()
				    .setSparkHome(Settings.sparkHome)
				    .setAppResource(Settings.applicationJar)
				    .setConf("spark.executor.memory", memory)
				    .setConf("spark.executor.cores", cores)
				    .setConf("spark.cores.max",  coresMax)
				    .addAppArgs(Settings.inputPathProfiler)
				    .addAppArgs(Settings.appArgs)
				    .addAppArgs(Settings.outputPath+"/"+Integer.toString(outputIndex))
				    .setMainClass(Settings.applicationClass).setMaster(Settings.sparkMaster).launch();

			}
			else
			{
				pr = new SparkLauncher()
					    .setSparkHome(Settings.sparkHome)
					    .setAppResource(Settings.applicationJar)
					    .setConf("spark.executor.memory", memory)
					    .setConf("spark.executor.cores", cores)
					    .setConf("spark.cores.max",  coresMax)
					    .addAppArgs(Settings.inputPathProfiler)
					    .addAppArgs(Settings.outputPath+"/"+Integer.toString(outputIndex))
					    .setMainClass(Settings.applicationClass).setMaster(Settings.sparkMaster).launch();
			}
					
			 
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
	        System.out.println("InputStream " + name + ":");
	        try {
	            String line = reader.readLine();
	            while (line != null) {
	                System.out.println(line);
	                line = reader.readLine();
	            }
	            reader.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}
	/*
	private void executeCommand1() {

		StringBuffer output = new StringBuffer();

		Process p;
		try {

			p = Runtime.getRuntime().exec("rm -rf /home/tawfiq/sp/spark-2.0.1/myoutput");
			p.waitFor();
			BufferedReader reader =
                            new BufferedReader(new InputStreamReader(p.getErrorStream()));

                        String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void executeCommand2() {

		StringBuffer output = new StringBuffer();

		Process p;
		try {

			p = Runtime.getRuntime().exec(new String[]{"ssh","-t","tawfiq@20.0.0.5","rm -rf /home/tawfiq/sp/spark-2.0.1/myoutput/"});
			p.waitFor();
			BufferedReader reader =
                            new BufferedReader(new InputStreamReader(p.getErrorStream()));

                        String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void executeCommand3() {

		StringBuffer output = new StringBuffer();

		Process p;
		try {

			p = Runtime.getRuntime().exec(new String[]{"ssh","-t","tawfiq@20.0.0.8","rm -rf /home/tawfiq/sp/spark-2.0.1/myoutput/"});
			p.waitFor();
			BufferedReader reader =
                            new BufferedReader(new InputStreamReader(p.getErrorStream()));

                        String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	private void executeCommand4() {

		StringBuffer output = new StringBuffer();

		Process p;
		try {

			p = Runtime.getRuntime().exec(new String[]{"ssh","-t","tawfiq@20.0.0.10","rm -rf /home/tawfiq/sp/spark-2.0.1/myoutput/"});
			p.waitFor();
			BufferedReader reader =
                            new BufferedReader(new InputStreamReader(p.getErrorStream()));

                        String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	public void submitApps(Configurations configObj)
	{		
		runCommand(Integer.toString(configObj.getCore()),Integer.toString(configObj.getMemory())+"g",Integer.toString(configObj.getMaxCore()),outputIndex++);
		//executeCommand1();
		//executeCommand2();
		//executeCommand3();
		//executeCommand4();	
	}
}
