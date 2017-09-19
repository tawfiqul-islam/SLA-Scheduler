package Profiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.json.*;

/*
 * Parses the Spark Master logs to get the completion time of the application with each config
 * 
 * @author: Muhammed Tawfiqul Islam
 */

public class LogParser {
	
	
	Configurations findConfig(int cores, int mem, int maxCores)
	{
		for(int i=0;i<Profiler.configList.size();i++)
		{
			if(cores==Profiler.configList.get(i).getCore()&&mem==Profiler.configList.get(i).getMemory()&&maxCores==Profiler.configList.get(i).getMaxCore())
			{
				return Profiler.configList.get(i);
			}
		}
		return null;
		
	}
	public void parseLog() {
		
		int cores = 0, mem = 0, maxCores = 0;
		String appID=null;

		//System.out.println(Settings.sparkHome+"/logs");
		File folder = new File(Settings.sparkHome+"/logs");
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				BufferedReader br = null;
				long startTime=0;
				long endTime = 0;
				try {
					br = new BufferedReader(new FileReader(Settings.sparkHome+"/logs/"+listOfFiles[i].getName()));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(listOfFiles[i].getName().contains("spark"))
					continue;
				String line;
				try {
					while ((line = br.readLine()) != null) {
						
						
						JSONObject obj = null, obj1=null;
						try {
							obj = new JSONObject(line);
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							String n = obj.getString("Event");
							if(n.equalsIgnoreCase("SparkListenerApplicationStart"))
							{
								startTime=obj.getLong("Timestamp");
							}
							if(n.equalsIgnoreCase("SparkListenerApplicationEnd"))
							{
								endTime=obj.getLong("Timestamp");
							}
							if(n.equalsIgnoreCase("SparkListenerEnvironmentUpdate"))
							{
		
								obj1=obj.getJSONObject("Spark Properties");
								appID = listOfFiles[i].getName();
								cores=obj1.getInt("spark.executor.cores");
								String memStr = obj1.getString("spark.executor.memory");
								mem=Integer.parseInt(memStr.substring(0, memStr.length()-1));
							    maxCores = obj1.getInt("spark.cores.max");
							}
								
							//System.out.println(n);
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				Configurations configObj=findConfig(cores,mem,maxCores);
				if(configObj!=null)
				{
					if(configObj.getAppID().size()==0)
					{
						configObj.addAppIDi(appID);
						configObj.addCompletionTimei(endTime-startTime);
					}
					else if(!configObj.findAppID(appID))
					{
						System.out.println(appID+" "+configObj.getCore()+" "+configObj.getMemory()+" "+configObj.getMaxCore());
						configObj.addAppIDi(appID);
						configObj.addCompletionTimei(endTime-startTime);
					}
				}
				//System.out.println("Application Completion Time="+(endTime-startTime)/1000+"s"+"\n\n");

			} 
		}

	}

}
