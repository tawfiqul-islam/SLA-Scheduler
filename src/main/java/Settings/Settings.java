package Settings;

/*
 * Holds the settings for the Profiler 
 * 
 * @author: Muhammed Tawfiqul Islam
 */
public class Settings {
	

	public static String sparkHome="/home/tawfiq/research/spark";
	public static String mesosMasterURI="http://127.0.0.1:5050";
	
	public static void printSettings() {
		System.out.println("sparkHome= "+sparkHome);
		System.out.println("mesosMasterURI= "+mesosMasterURI);
	}
}
