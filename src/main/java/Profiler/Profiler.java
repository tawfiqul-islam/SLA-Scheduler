package Profiler;

import Settings.Settings;
import Settings.SettingsLoader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/*The main class for the SparkProfiler
 * 
 * @author: Muhammed Tawfiqul Islam
 * 
 */
public class Profiler {

	static ArrayList<Configurations> configList;
	static ConfigurationGenerator configGenObj;
	static ProfilerDeployer profDepObj;
	static LogParser logParserObj;
	static File inputDirectory;
	static ArrayList<Double> inputSizes;
	
	static void printConfigList()
	{
		for(int i=0;i<configList.size();i++)
		{
			configList.get(i).printConfig();
		}
	}

	static void init() {

		//Load Settings for Profiler
		SettingsLoader.loadSettings();
		
		configList=new ArrayList<Configurations>();
		configGenObj=new ConfigurationGenerator();;
		inputSizes = new ArrayList<Double>();
		profDepObj = new ProfilerDeployer();
		logParserObj = new LogParser();
		inputDirectory = new File(Settings.inputPathProfiler);

		//Generate configurations of application for profiler
		configGenObj.generateAppConfig();
	}
	
	static void ApplicationProfile()
	{
		//initialize everything
		init();
		//Print Settings
		Settings.printSettings();
		
		for(int k=0;k<Settings.reProfile;k++)
		{
			if(k!=0)
			{
				//make profiling input directory 2x of current size
				make2xInputSize(k);
			}
			inputSizes.add((double)inputSize()/1024.0/1024.0);
			System.out.println("***Profiling Input Directory Size: "+inputSizes.get(k)+"MB");
			for(int i=0;i<configList.size();i++)
			{
				for(int j=0;j<Settings.repeatConfig;j++)
				{
					profDepObj.submitApps(configList.get(i));
					logParserObj.parseLog();
				}
			}	
		}
		System.out.println("Profiling input sizes: ");
		for(int i=0;i<Settings.reProfile;i++)
		{
			System.out.println(inputSizes.get(i)+" MB");
		}	
	}

	private static void make2xInputSize(int i) {
		for (File file : inputDirectory.listFiles()) {
			if (file.isFile())
			{
				runCommand("cp "+file+" "+file+i);
			}
		}
	}

	public static long inputSize() {
		long length = 0;
		for (File file : inputDirectory.listFiles()) {
			if (file.isFile())
				length += file.length();
		}
		return length;
	}

	static void output()
	{
		FileWriter fw = null;
		try {
			fw = new FileWriter("output.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=0;i<Profiler.configList.size();i++)
		{
			for(int j=0;j<Profiler.configList.get(i).completionTime.size();j++)
			{
				try {
					fw.write(Profiler.configList.get(i).completionTime.get(j)/1000+" "+Profiler.configList.get(i).getTotalCores()+" "+Profiler.configList.get(i).getTotalMemory()+" "+inputSizes.get(j/Settings.reProfile)+" "+Profiler.configList.get(i).getTotalExecs()+"\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		try {
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	static void runCommand(String cmd)
	{
		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			pr.waitFor();
			//break;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String args[])
	{
		ApplicationProfile();
		printConfigList();
	}
}

