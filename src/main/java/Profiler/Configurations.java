package Profiler;

import java.util.ArrayList;
import java.util.Comparator;

/*
 * Holds the configurations needed for launching a Spark application on the cluster
 * It is also responsible for holding completion time of application with each config
 * 
 * @author: Muhammed Tawfiqul Islam
 */
public class Configurations implements Comparator<Configurations>{

	private ArrayList<String> appID = new ArrayList<String>();
	private int core;
	private int memory;
	private int maxCore;
	public ArrayList<Double> completionTime = new ArrayList<Double>();
	private int priority;
	private int totalMemory;
	private int totalCores;
	private int totalExecs;
	private String submitStr;	
	private boolean isSuccessful;
	private double p1;
	private double p2;
	private double cost;

	public ArrayList<String> getAppID() {
		return appID;
	}
	public void setAppID(ArrayList<String> appID) {
		this.appID = appID;
	}
	public String getAppIDi(int i) {
		return appID.get(i);
	}
	public void addAppIDi(String appID) {
		this.appID.add(appID);
	}
	public double getCompletionTimei(int i) {
		return completionTime.get(i);
	}
	public void addCompletionTimei(double val) {
		this.completionTime.add(val);
	}
	public int getCore() {
		return core;
	}
	public void setCore(int core) {
		this.core = core;
	}
	public int getMemory() {
		return memory;
	}
	public void setMemory(int memory) {
		this.memory = memory;
	}
	public int getMaxCore() {
		return maxCore;
	}
	public void setMaxCore(int maxCore) {
		this.maxCore = maxCore;
	}
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	public boolean isSuccessful() {
		return isSuccessful;
	}
	public void setSuccessful(boolean isSuccessful) {
		this.isSuccessful = isSuccessful;
	}
	public String getSubmitStr() {
		return submitStr;
	}
	public void setSubmitStr(String submitStr) {
		this.submitStr = submitStr;
	}

	public int getTotalMemory() {
		return totalMemory;
	}
	public void setTotalMemory(int totalMemory) {
		this.totalMemory = totalMemory;
	}
	public int getTotalCores() {
		return totalCores;
	}
	public void setTotalCores(int totalCores) {
		this.totalCores = totalCores;
	}

	public int getTotalExecs() {
		return totalExecs;
	}
	public void setTotalExecs(int totalExecs) {
		this.totalExecs = totalExecs;
	}
	
	public double getP1() {
		return p1;
	}
	public void setP1(double p1) {
		this.p1 = p1;
	}
	public double getP2() {
		return p2;
	}
	public void setP2(double p2) {
		this.p2 = p2;
	}
	
	public double getCost() {
		return cost;
	}
	public void setCost(double cost) {
		this.cost = cost;
	}

	@Override// Overriding the compare method to sort by cost 
	public int compare(Configurations d1, Configurations d2) {
	        if (d1.cost < d2.cost) return -1;
	        if (d1.cost > d2.cost) return 1;
	        return 0;
	}

	public boolean findAppID(String str)
	{
		for(int i=0;i<appID.size();i++)
			if(appID.get(i).equalsIgnoreCase(str))
			{
				return true;
			}
		return false;
	}
	
	public void printConfig()
	{	
		System.out.println("Total Cores: "+totalCores);
		System.out.println("Total Memory: "+totalMemory);
		System.out.println("Total Executors: "+totalExecs);
		
		for(int i=0;i<appID.size();i++)
		{
			System.out.println("*Application ID "+(i+1)+": "+appID.get(i));
		}

		for(int i=0;i<completionTime.size();i++)
		{
			System.out.println("*Completion Time "+(i+1)+": "+completionTime.get(i)/1000+" s");
		}
		double avg=0;
		for(int i=0;i<completionTime.size();i++)
		{
			avg+=completionTime.get(i)/1000;
			if(i!=0&&(i+1)%Settings.repeatConfig==0)
			{
				System.out.println("avg Completion Time: "+avg/Settings.repeatConfig+"s");
				avg=0;
			}
		}	
	}
}
