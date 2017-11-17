package Settings;

/*
 * Holds the settings for the Profiler 
 * 
 * @author: Muhammed Tawfiqul Islam
 */
public class Settings {
	

	public static String sparkHome;
	public static String mesosMasterURI;
	
	public static void printSettings() {
		System.out.println("sparkHome= "+sparkHome);
		System.out.println("mesosMasterURI= "+mesosMasterURI);
	}
}
