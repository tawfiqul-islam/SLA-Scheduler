package Cluster;

public class Agent {

    private double cpu;
    private double mem;
    private double disk;
    private int portStart;
    private int portEnd;
    private boolean isAlive;
    private String id;
    private long registeredTime;

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

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
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

    @Override
    public String toString() {
        return "{" +
                "id='" + id + '\'' +
                ", cpu=" + cpu +
                ", mem=" + mem +
                ", disk=" + disk +
                ", portStart=" + portStart +
                ", portEnd=" + portEnd +
                ", isAlive=" + isAlive +
                ", registeredTime=" + registeredTime +
                '}';
    }
}
