package JobMananger;

import Scheduler.SchedulerUtil;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobRequestHandler extends Thread {

    private static final Logger logger = Logger.getLogger( JobRequestHandler.class.getName());

    public ServerSocket listenerSocket = null;
    private boolean running;
    private Socket clientSocket;
    private JobProcessor jobProcessorObj;
    public void startSocket()
    {
        try {
            listenerSocket = new ServerSocket();

            listenerSocket.bind(new InetSocketAddress(SchedulerUtil.schedulerIP, SchedulerUtil.jobHandlerPort));

            logger.info("Started JobHandler on Port: " + SchedulerUtil.jobHandlerPort);
        }catch (Exception e) {
            logger.log(Level.SEVERE,"Exception in starting JobHandler on Port: " + SchedulerUtil.jobHandlerPort,e);
        }
    }

    @Override
    public void run() {
        startSocket();
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            logger.log(Level.SEVERE,"Interrupted JobHandler on port: " + SchedulerUtil.jobHandlerPort,e);
        }

        running = true;

        while(running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE,"Interrupted JobHandler on port: " + SchedulerUtil.jobHandlerPort,e);
            }

            try {
                clientSocket = listenerSocket.accept();

                logger.info("Got a new request on port: " + SchedulerUtil.jobHandlerPort + " in JobHandler from " + clientSocket.getRemoteSocketAddress());

                jobProcessorObj = new JobProcessor(clientSocket);
                jobProcessorObj.start();

            } catch (Throwable e) {
                logger.log(Level.SEVERE,"Exception in JobHandler running in port: " + SchedulerUtil.jobHandlerPort ,e);
                running=false;
                break;
            }
        }
    }
    public boolean isRunning()
    {
        return running;
    }

    public void shutDown() {
        running = false;
        try {

            listenerSocket.close();
            logger.info("JobHandler shutDown successfully on port: " + SchedulerUtil.jobHandlerPort);

        } catch (Exception e) {
            logger.log(Level.SEVERE,"Exception in shutting down JobHandler", e);
        }
    }
}