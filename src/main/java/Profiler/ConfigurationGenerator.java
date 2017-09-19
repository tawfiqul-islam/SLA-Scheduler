package Profiler;

import java.io.FileWriter;
import java.io.IOException;

/*
 * Generates configurations (depending on profiling level[high/low] and 
 * generates application submission commands for Spark)
 * 
 * @author: Muhammed Tawfiqul Islam
 */
public class ConfigurationGenerator {

    int coreStarter=1;
	int sparkExecutorCores;
	int sparkExecutorMemory;
	int sparkCoresMax;
	int sparkExecutorsPerWorker;
	
	public static int applicationCounter=0;
	
	public void generateAppConfig()
	{
		int Ce, Emax, Me, E;
		FileWriter fw = null;
		try {
			fw = new FileWriter("applicationConfig.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(Settings.profilerLevel.equalsIgnoreCase("low"))
		{
			coreStarter=Settings.executorCoresLimit;
			System.out.println("yes");
		}
		else
		{
			coreStarter=1;
			System.out.println("no");
		}
		for(int i=coreStarter;i<=Settings.executorCoresLimit;i++)
		{
			Ce=i;
			Me=Ce*(Settings.workerMemory/Settings.workerCores);
			Emax=Settings.workerNumbers*(Settings.workerCores/Ce);
			for(E=1;E<=Emax;E++)
			{
				try {
					Configurations configObj = new Configurations();
					configObj.setCore(Ce);
					configObj.setMemory(Me);
					configObj.setMaxCore(Ce*E);	
					
					configObj.setTotalExecs(E);
					configObj.setTotalCores(Ce*E);
					configObj.setTotalMemory(Me*E);
					configObj.setCost(Settings.coreCost*Ce*E);
					Profiler.configList.add(configObj);
					fw.write(Ce+" "+Me+" "+E+"\n");
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
}
