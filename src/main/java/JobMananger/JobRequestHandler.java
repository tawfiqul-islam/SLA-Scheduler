package JobMananger;

import Scheduler.SchedulerUtil;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;

public class JobRequestHandler extends Thread {


    public ServerSocket listenerSocket = null;
    private boolean running;
    private Socket clientSocket;
    private JobProcessor jobProcessorObj;
    public void startSocket()
    {
        try {
            listenerSocket = new ServerSocket();

            listenerSocket.bind(new InetSocketAddress(SchedulerUtil.schedulerIP, SchedulerUtil.jobHandlerPort));

            Log.SchedulerLogging.log(Level.INFO,JobRequestHandler.class.getName()+" Started JobHandler on Port: " + SchedulerUtil.jobHandlerPort);

        }catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,JobRequestHandler.class.getName()+" Exception in startSocket: " + e.toString());
        }
    }

    @Override
    public void run() {
        startSocket();
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,JobRequestHandler.class.getName()+" Exception in run method: " + e.toString());
        }

        running = true;

        while(running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Log.SchedulerLogging.log(Level.SEVERE,JobRequestHandler.class.getName()+" Exception in run method: " + e.toString());
            }

            try {
                clientSocket = listenerSocket.accept();

                Log.SchedulerLogging.log(Level.INFO,JobRequestHandler.class.getName()+" Got a new request on port: " + SchedulerUtil.jobHandlerPort + " in JobHandler from " + clientSocket.getRemoteSocketAddress());

                jobProcessorObj = new JobProcessor(clientSocket);
                jobProcessorObj.start();

            } catch (Throwable e) {
                Log.SchedulerLogging.log(Level.SEVERE,JobRequestHandler.class.getName()+" Exception in run method: " + e.toString());
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
            Log.SchedulerLogging.log(Level.INFO,JobRequestHandler.class.getName()+" JobHandler shutDown successfully on port: " + SchedulerUtil.jobHandlerPort);

        } catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,JobRequestHandler.class.getName()+" Exception in jobHandler shutDown: " + e.toString());
        }
    }
}