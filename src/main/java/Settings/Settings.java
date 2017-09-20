package Settings;

/*
 * Holds the settings for the Profiler 
 * 
 * @author: Muhammed Tawfiqul Islam
 */
public class Settings {
	
	public static int workerNumbers;
	public static int workerCores;
	public static int workerMemory;
	public static int executorCoresLimit;
	public static int profilerInputSize;
	public static String profilerLevel;
	public static String sparkHome;
	public static String inputPathProfiler;
	public static String inputPathApplication;
	public static String applicationJar;
	public static String applicationClass;
	public static String appArgs;
	public static String outputPath;
	public static String sparkMaster;
	public static int reProfile;
	public static int repeatConfig;
	public static double coreCost;
	
	
	public static void printSettings()
	{
		System.out.println("Total Worker Nodes="+workerNumbers);
		System.out.println("Cores in Each Worker="+workerCores);
		System.out.println("Memory in Each Worker="+workerMemory+"GB");
		System.out.println("Maximum Cores Limit per Executor="+executorCoresLimit);
		System.out.println("Profiler Input Size="+profilerInputSize+"GB");
		System.out.println("Reprofile size="+reProfile);
		System.out.println("Profiler Level="+profilerLevel);
		System.out.println("Spark Home="+sparkHome);
		System.out.println("Profiler Input Path="+inputPathProfiler);
		System.out.println("Application Input Path="+inputPathApplication);
		System.out.println("Application Jar="+applicationJar);
		System.out.println("Application Class="+applicationClass);
		System.out.println("OutputPath="+outputPath);
		System.out.println("Spark Master="+sparkMaster);
		System.out.println("Core Cost="+coreCost);
		
	}
}
