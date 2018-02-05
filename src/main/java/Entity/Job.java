package Entity;

import Scheduler.SchedulerUtil;

import java.util.ArrayList;

public class Job {

    private String jobID;
    private String role;
    private String frameworkID;
    private int allocatedExecutors;
    //stats
    private String arrivalTime;
    private double startTime;
    private double finishTime;
    private double totalPredictedTime;
    private boolean isSubmitted;
    private boolean isAlive;
    private boolean isSuccessful;
    private ArrayList<String> agentList = new ArrayList<>();

    private int priority;

    //Resource Requirements
    private int executors;
    private int coresPerExecutor;
    private double memPerExecutor;
    private double executorMemoryOverhead;
    private double totalExecutorMemory;
    private double inputSize;
    private double resourceSplit;

    //Environment Information
    private String inputPath;
    private String outputPath;
    private String appJarPath;
    private String mainClassName;
    private String appArgs;



    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getFrameworkID() {
        return frameworkID;
    }

    public void setFrameworkID(String frameworkID) {
        this.frameworkID = frameworkID;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getAllocatedExecutors() {
        return allocatedExecutors;
    }

    public void setAllocatedExecutors(int allocatedExecutors) {
        this.allocatedExecutors = allocatedExecutors;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public double getStartTime() {
        return startTime;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    public double getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(double finishTime) {
        this.finishTime = finishTime;
    }

    public double getTotalPredictedTime() {
        return totalPredictedTime;
    }

    public void setTotalPredictedTime(double totalPredictedTime) {
        this.totalPredictedTime = totalPredictedTime;
    }

    public boolean isSubmitted() {
        return isSubmitted;
    }

    public void setSubmitted(boolean submitted) {
        isSubmitted = submitted;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }

    public void setSuccessful(boolean successful) {
        isSuccessful = successful;
    }

    public ArrayList<String> getAgentList() {
        return agentList;
    }

    public void setAgentList(ArrayList<String> agentList) {
        this.agentList = agentList;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getExecutors() {
        return executors;
    }

    public void setExecutors(int executors) {
        this.executors = executors;
    }

    public int getCoresPerExecutor() {
        return coresPerExecutor;
    }

    public void setCoresPerExecutor(int coresPerExecutor) {
        this.coresPerExecutor = coresPerExecutor;
    }

    public double getMemPerExecutor() {
        return memPerExecutor;
    }

    public void setMemPerExecutor(double memPerExecutor) {
        this.memPerExecutor = memPerExecutor;
    }


    public void setExecutorMemoryOverhead() {
        executorMemoryOverhead=((memPerExecutor * 0.10) > 384.0)?(memPerExecutor * 0.10):384.0;
        setTotalExecutorMemory();
    }
    public void setTotalExecutorMemory() {
        totalExecutorMemory=memPerExecutor+executorMemoryOverhead;
    }

    public double getTotalExecutorMemory() {
        return totalExecutorMemory;
    }

    public double getInputSize() {
        return inputSize;
    }

    public void setInputSize(double inputSize) {
        this.inputSize = inputSize;
    }

    public double getResourceSplit() {
        return resourceSplit;
    }

    public void setResourceSplit() {
        this.resourceSplit = this.coresPerExecutor * this.executors + (this.memPerExecutor * this.executors) * SchedulerUtil.resourceSplitThreshold;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getAppJarPath() {
        return appJarPath;
    }

    public void setAppJarPath(String appJarPath) {
        this.appJarPath = appJarPath;
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public void setMainClassName(String mainClassName) {
        this.mainClassName = mainClassName;
    }

    public String getAppArgs() {
        return appArgs;
    }

    public void setAppArgs(String appArgs) {
        this.appArgs = appArgs;
    }

    @Override
    public String toString() {
        return "Job{" +
                "jobID='" + jobID + '\'' +
                ", role='" + role + '\'' +
                ", arrivalTime='" + arrivalTime + '\'' +
                ", executors=" + executors +
                ", coresPerExecutor=" + coresPerExecutor +
                ", memPerExecutor=" + memPerExecutor +
                ", executorMemoryOverhead=" + executorMemoryOverhead +
                ", inputPath='" + inputPath + '\'' +
                ", outputPath='" + outputPath + '\'' +
                ", appJarPath='" + appJarPath + '\'' +
                ", mainClassName='" + mainClassName + '\'' +
                ", appArgs='" + appArgs + '\'' +
                '}';
    }

}
