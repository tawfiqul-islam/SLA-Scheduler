package Log;
import Scheduler.SchedulerUtil;

import java.io.IOException;
import java.util.logging.*;

public class SchedulerLogging {
    static Logger logger;
    public Handler fileHandler;
    Formatter plainText;

    private SchedulerLogging() throws IOException {

        logger = Logger.getLogger(SchedulerLogging.class.getName());
        fileHandler = new FileHandler(SchedulerUtil.schedulerHome+"/logs/schedulerLog.txt", true);
        plainText = new SimpleFormatter();
        fileHandler.setFormatter(plainText);
        logger.addHandler(fileHandler);
    }

    private static Logger getLogger(){
        if(logger == null){
            try {
                new SchedulerLogging();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return logger;
    }

    public static void log(Level level, String msg){
        getLogger().log(level, msg);
    }
}