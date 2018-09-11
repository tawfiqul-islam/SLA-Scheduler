package Monitor;

import Entity.Executor;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.ArrayList;

public class ExecutorTracker extends Thread {

    public static boolean shutDown=false;
    public void run() {

        Executor tmpExecutor,executorObj;
        while(true) {

            if(shutDown) {
                break;
            }
            for (int i = 0;i<MonitorManager.slaveIPPort.size();i++ ) {

                ServerResponse obj = HTTPAPI.HTTPGETSender("http://"+MonitorManager.slaveIPPort.get(i)+"/monitor/statistics");
                ArrayList<Executor> executorList = HTTPAPI.parseExecutorMetrics(obj.getResponseString(),MonitorManager.slaveIPPort.get(i));

                for(int j=0;j<executorList.size();j++) {
                    executorObj=executorList.get(j);
                    String key = executorObj.getFrameworkID() + executorObj.getExecutorID();
                    if (MonitorManager.executorTrackerMap.containsKey(key)) {

                        tmpExecutor = MonitorManager.executorTrackerMap.get(key);
                        if (tmpExecutor.getMEMMaxUsage() > executorObj.getMEMMaxUsage())
                            executorObj.setMEMMaxUsage(tmpExecutor.getMEMMaxUsage());
                        if (tmpExecutor.getMEMMinUsage() < executorObj.getMEMMinUsage())
                            executorObj.setMEMMinUsage(tmpExecutor.getMEMMinUsage());
                        executorObj.setMEMTotalUsage(executorObj.getMEMTotalUsage() + tmpExecutor.getMEMTotalUsage());
                        executorObj.setTotalObservations(tmpExecutor.getTotalObservations() + 1);

                        MonitorManager.executorTrackerMap.put(key, executorObj);
                    } else {
                        MonitorManager.executorTrackerMap.put(key, executorObj);
                    }
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
