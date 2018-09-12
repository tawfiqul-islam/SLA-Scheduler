package Monitor;

import Entity.Executor;
import Operator.HTTPAPI;
import Operator.ServerResponse;
import Settings.Settings;

import java.util.ArrayList;
import java.util.HashMap;

public class MonitorManager {

    public static HashMap<String, Executor> executorTrackerMap = new HashMap<>();
    public static ArrayList<String> slaveIPPort = new ArrayList<>();

    public static void monitorJobs() {

        ServerResponse obj = HTTPAPI.HTTPGETSender(Settings.mesosMasterURI+"/master/slaves");
        HTTPAPI.parseSlaves(obj.getResponseString());

        ExecutorTracker executorTrackerObj = new ExecutorTracker();
        executorTrackerObj.start();
        /*obj = HTTPAPI.HTTPGETSender("http://" + MonitorManager.slaveIPPort.get(0) + "/monitor/statistics");
        //obj = HTTPAPI.HTTPGETSender("http://115.146.85.66:5051/monitor/statistics");
        ArrayList<Executor> executorList = HTTPAPI.parseExecutorMetrics(obj.getResponseString(),"115.146.92.100:5050");

        for (int j = 0; j < executorList.size(); j++) {
            System.out.println(executorList.get(j).toString());
        }*/
    }
}
