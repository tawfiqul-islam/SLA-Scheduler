package Entity;

public class Executor {
    private int ExecutorID;
    private String executorName;
    private String frameworkID;
    private double latestTimeStamp;
    private double CPULim;
    private double MEMLim;
    private double CPUTimeUser;
    private double CPUTimeSystem;
    private double CPUUtilization;
    private double MEMMaxUsage;
    private double MEMMinUsage;
    private double MEMTotalUsage;
    private double MEMMeanUsage;
    private long totalObservations;
    private String hostVM;

    public int getExecutorID() {
        return ExecutorID;
    }

    public void setExecutorID(int executorID) {
        ExecutorID = executorID;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getFrameworkID() {
        return frameworkID;
    }

    public void setFrameworkID(String frameworkID) {
        this.frameworkID = frameworkID;
    }

    public double getLatestTimeStamp() {
        return latestTimeStamp;
    }

    public void setLatestTimeStamp(double latestTimeStamp) {
        this.latestTimeStamp = latestTimeStamp;
    }

    public double getCPULim() {
        return CPULim;
    }

    public void setCPULim(double CPULim) {
        this.CPULim = CPULim;
    }

    public double getMEMLim() {
        return MEMLim;
    }

    public void setMEMLim(double MEMLim) {
        this.MEMLim = MEMLim;
    }

    public double getCPUTimeUser() {
        return CPUTimeUser;
    }

    public void setCPUTimeUser(double CPUTimeUser) {
        this.CPUTimeUser = CPUTimeUser;
    }

    public double getCPUTimeSystem() {
        return CPUTimeSystem;
    }

    public void setCPUTimeSystem(double CPUTimeSystem) {
        this.CPUTimeSystem = CPUTimeSystem;
    }

    public double getCPUUtilization() {
        return CPUUtilization;
    }

    public void setCPUUtilization(double CPUUtilization) {
        this.CPUUtilization = CPUUtilization;
    }

    public double getMEMMaxUsage() {
        return MEMMaxUsage;
    }

    public void setMEMMaxUsage(double MEMMaxUsage) {
        this.MEMMaxUsage = MEMMaxUsage;
    }

    public double getMEMMinUsage() {
        return MEMMinUsage;
    }

    public void setMEMMinUsage(double MEMMinUsage) {
        this.MEMMinUsage = MEMMinUsage;
    }

    public double getMEMTotalUsage() {
        return MEMTotalUsage;
    }

    public void setMEMTotalUsage(double MEMTotalUsage) {
        this.MEMTotalUsage = MEMTotalUsage;
    }

    public double getMEMMeanUsage() {
        return MEMMeanUsage;
    }

    public void setMEMMeanUsage(double MEMMeanUsage) {
        this.MEMMeanUsage = MEMMeanUsage;
    }

    public long getTotalObservations() {
        return totalObservations;
    }

    public void setTotalObservations(long totalObservations) {
        this.totalObservations = totalObservations;
    }

    public String getHostVM() {
        return hostVM;
    }

    public void setHostVM(String hostVM) {
        this.hostVM = hostVM;
    }

    @Override
    public String toString() {
        return "Executor{" +
                "ExecutorID=" + ExecutorID +
                ", executorName='" + executorName + '\'' +
                ", frameworkID='" + frameworkID + '\'' +
                ", latestTimeStamp=" + latestTimeStamp +
                ", CPULim=" + CPULim +
                ", MEMLim=" + MEMLim +
                ", CPUTimeUser=" + CPUTimeUser +
                ", CPUTimeSystem=" + CPUTimeSystem +
                ", MEMMaxUsage=" + MEMMaxUsage +
                ", MEMMinUsage=" + MEMMinUsage +
                ", MEMTotalUsage=" + MEMTotalUsage +
                '}';
    }

    /*
cpus_total_usage = (
                    (b.cpus_system_time_secs - a.cpus_system_time_secs) +
                    (b.cpus_user_time_secs - a.cpus_user_time_secs)) /
                    (b.timestamp - a.timestamp)
                   )
cpu_percent      = cpus_total_usage / cpu_limit * 100%
* */

}
