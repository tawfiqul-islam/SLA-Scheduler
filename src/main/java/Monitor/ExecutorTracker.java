package Monitor;

import Entity.Executor;
import Operator.HTTPAPI;
import Operator.ServerResponse;

import java.util.ArrayList;

public class ExecutorTracker extends Thread {

    public void run() {

        Executor tmpExecutor,executorObj;
        while(true) {
            for (int i = 0;i<MonitorManager.slaveIPPort.size();i++ ) {

                ServerResponse obj = HTTPAPI.HTTPGETSender("http://"+MonitorManager.slaveIPPort.get(i)+"/monitor/statistics");
                ArrayList<Executor> executorList = HTTPAPI.parseExecutorMetrics(obj.getResponseString());

                for(int j=0;j<executorList.size();j++) {
                    executorObj=executorList.get(j);
                    String key = executorObj.getFrameworkID() + executorObj.getExecutorID();
                    if (MonitorManager.executorTracker.containsKey(key)) {

                        tmpExecutor = MonitorManager.executorTracker.get(key);
                        if (tmpExecutor.getMEMMaxUsage() > executorObj.getMEMMaxUsage())
                            executorObj.setMEMMaxUsage(tmpExecutor.getMEMMaxUsage());
                        if (tmpExecutor.getMEMMinUsage() < executorObj.getMEMMinUsage())
                            executorObj.setMEMMinUsage(tmpExecutor.getMEMMinUsage());
                        executorObj.setMEMTotalUsage(executorObj.getMEMTotalUsage() + tmpExecutor.getMEMTotalUsage());
                        executorObj.setTotalObservations(tmpExecutor.getTotalObservations() + 1);

                        MonitorManager.executorTracker.put(key, executorObj);
                    } else {
                        MonitorManager.executorTracker.put(key, executorObj);
                    }
                }
            }
        }
    }
}
