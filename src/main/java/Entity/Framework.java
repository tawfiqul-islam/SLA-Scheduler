package Entity;

import java.util.ArrayList;

public class Framework {

    private long startTime;
    private long finishTime;
    private String role;
    private String ID;
    private boolean active;
    private int executors;
    private ArrayList<Executor> executorList = new ArrayList<>();
    private double CPUMeanUtilization;
    private double MEMMaxUsage;
    private double MEMMinUsage;
    private double MEMMeanUsage;


    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getExecutors() {
        return executors;
    }

    public void setExecutors(int executors) {
        this.executors = executors;
    }
}

