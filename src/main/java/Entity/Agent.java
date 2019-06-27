package Entity;

import Scheduler.SchedulerUtil;

public class Agent {

    private double defaultCPU;
    private double defaultMEM;
    private double cpu;
    private double mem;
    private double resourceTotal;
    private double disk;
    private int portStart;
    private int portEnd;
    private boolean isUsed;
    private boolean isActive;
    private String id;
    private long registeredTime;
    private double weight;
    private long runTime=0;
    private double price;
    private boolean isLocal;

    public boolean isLocal() {
        return isLocal;
    }

    public void setLocal(boolean local) {
        isLocal = local;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public double getDefaultCPU() {
        return defaultCPU;
    }

    public void setDefaultCPU(double defaultCPU) {
        this.defaultCPU = defaultCPU;
    }

    public double getDefaultMEM() {
        return defaultMEM;
    }

    public void setDefaultMEM(double defaultMEM) {
        this.defaultMEM = defaultMEM;
    }

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public double getMem() {
        return mem;
    }

    public void setMem(double mem) {
        this.mem = mem;
    }

    public double getResourceTotal() {
        setResourceTotal();
        return resourceTotal;
    }

    public void setResourceTotal() {
        this.resourceTotal = cpu * (1-SchedulerUtil.resourceSplitThreshold) + mem * SchedulerUtil.resourceSplitThreshold;
    }

    public double getDisk() {
        return disk;
    }

    public void setDisk(double disk) {
        this.disk = disk;
    }

    public int getPortStart() {
        return portStart;
    }

    public void setPortStart(int portStart) {
        this.portStart = portStart;
    }

    public int getPortEnd() {
        return portEnd;
    }

    public void setPortEnd(int portEnd) {
        this.portEnd = portEnd;
    }

    public boolean isUsed() {
        return isUsed;
    }

    public void setUsed(boolean used) {
        isUsed = used;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getRegisteredTime() {
        return registeredTime;
    }

    public void setRegisteredTime(long registeredTime) {
        this.registeredTime = registeredTime;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double cpu, double mem) {
        double cpuWeight = this.cpu/cpu;
        double memWeight = this.mem/mem;
        this.weight = cpuWeight<memWeight?cpuWeight:memWeight;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    @Override
    public String toString() {
        return "{" +
                "id='" + id + '\'' +
                ", cpu=" + cpu +
                ", mem=" + mem +
                ", disk=" + disk +
                ", portStart=" + portStart +
                ", portEnd=" + portEnd +
                ", isActive=" + isActive +
                ", registeredTime=" + registeredTime +
                '}';
    }
}
