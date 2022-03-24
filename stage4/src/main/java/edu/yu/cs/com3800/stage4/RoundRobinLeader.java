package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RoundRobinLeader extends Thread implements LoggingServer {

    Logger logger;
    int myPort;
    boolean removedGateway = false;
    static int gatewayPort;
    static String gatewayHost = "localhost";
    Map<Long, InetSocketAddress> peerIDtoAddress;
    ZooKeeperPeerServerImpl server;
    ArrayList<InetSocketAddress> workers;
    ServerSocket socket = null;


    public RoundRobinLeader(ZooKeeperPeerServerImpl server, Map<Long, InetSocketAddress> peerIDtoAddress) {

        this.peerIDtoAddress = peerIDtoAddress;
        this.myPort = server.getUdpPort();
        this.server = server;
        this.setDaemon(true);
        setName("RoundRobinLeader-port-" + this.myPort);
        this.logger = initializeLogging(RoundRobinLeader.class.getName() + "-on-port-" + this.myPort);
        logger.info("starting master worker. server id  " + this.server.getServerId());
        this.workers = new ArrayList<>(this.peerIDtoAddress.values());
    }


    @Override
    public void run() {

        try {

            int count = -1; // keep track of available workers.
            int con = myPort+2;
            logger.info("receiving TCP connection on port - " + con);
            socket = new ServerSocket(this.myPort + 2);  //Get the msg from Gateway
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

            while (!this.isInterrupted()) {

                if (count >= workers.size() - 1) count = -1; // reset loop of available workers

                Message msg = null;
                byte[] receiveBuf = new byte[400];  // Receiver buffer
                Socket gatewaySocket = socket.accept();     // TCP connection with Gateway
                InputStream gatewayIn = gatewaySocket.getInputStream();
                OutputStream gatewayOut = gatewaySocket.getOutputStream();

                //Step 1: Receive message from Gateway
                gatewayIn.read(receiveBuf);
                msg = new Message(receiveBuf);
                logger.info("Received Message from Gateway: \n" + msg.toString());

                // Step 2:  remove gateway from the list of available workers
                removeGateway(msg);

                // Step 3: find next available worker.
                InetSocketAddress worker = workers.get(++count);
                int p = worker.getPort()+2;
                logger.info("Sending work to JavaRunnerWorker in TCP port: " + p);
                Message sending = new Message(Message.MessageType.WORK, msg.getMessageContents(), this.server.getAddress().getHostString(), this.myPort+2, worker.getHostString(), p);

                // Start new worker manager Thread. it will send work to the worker and wait for the response.
                workersManager workerThread = new workersManager(sending, worker, gatewayOut);
                logger.info("Creating Work Thread");
                executor.execute(workerThread);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        logger.info("Shutdown RoundRobinLeader");
        try {
          this.socket.close();
          //sleep(3000);
        } catch (IOException  e) {
            e.printStackTrace();
        }
        this.interrupt();
    }

    protected void removeGateway(Message msg) {

        if (removedGateway) return; // we already removed the gateway

        for (InetSocketAddress worker : workers) { //
            if (msg.getMessageType().equals(Message.MessageType.WORK) && worker.getPort() == msg.getSenderPort()) {
                workers.remove(worker);
                gatewayPort = worker.getPort();
                removedGateway = true;
                break;
            }
        }
    }
}

     class workersManager extends Thread{

       Message msg;
       InetSocketAddress worker;
       OutputStream gatewayOut;
       Logger logger;

       workersManager(Message msg,  InetSocketAddress worker,  OutputStream  gatewayOut){
           this.msg = msg;
         this.worker = worker;
         this.setDaemon(true);
         this.setName("WorkerManager for worker on port" + worker.getPort());
         this.gatewayOut = gatewayOut;
         this.logger = initializeLogging("WorkersManagerThread sending to port" + worker.getPort());
         logger.info("Starting WorkerManager");

       }


        @Override
        public void run() {

            try {

            int workerPort = worker.getPort() +2;
            Socket workerSocket = new Socket(worker.getHostName(), worker.getPort()+2);
            OutputStream workerOut = workerSocket.getOutputStream();
            InputStream workerIn = workerSocket.getInputStream();
            logger.info("Establishing TCP  connection with Server on port " + workerPort);

            // Step 1: Send TCP message to JavaRunnerFollower.
            byte [] data = msg.getNetworkPayload();
            workerOut.write(data);
            logger.info("Work sent to JavaRunnerFollower");


            // Step 2: Receive response from JavaRunnerFollower
            logger.info("receiving response from worker");
            byte[] receiveBuf = new byte[400];
            workerIn.read(receiveBuf);
            Message workerResponse = new Message(receiveBuf);
            logger.info( "Received response from worker on TCP port:  "+ workerPort +"\n" + workerResponse.toString());


           //Step 3: Send response back to Gateway.
            Message answer = new Message(Message.MessageType.COMPLETED_WORK, workerResponse.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(), RoundRobinLeader.gatewayHost, RoundRobinLeader.gatewayPort,0,workerResponse.getErrorOccurred());
            gatewayOut.write(answer.getNetworkPayload());
            logger.info("response sent to Gateway:\n" + answer.toString());
            workerSocket.close();


            } catch (IOException e) {
                logger.info("CATCH \n " + Util.getStackTrace(e));
                e.printStackTrace();
            }
        }



         protected Logger initializeLogging(String fileNamePreface ) {

//        if (logger != null)return this.logger;

             boolean disableParentHandlers = true;
             Logger logger = Logger.getLogger(fileNamePreface);
             logger.setLevel(Level.FINER);

             FileHandler fh = null;
             try {
                 fileNamePreface.replace("edu.yu.cs.com3800", "");
                 fh = new FileHandler("./Log/LogFile_" + fileNamePreface  +".log");
             } catch (IOException e) {
                 e.printStackTrace();
             }
             fh.setLevel(Level.INFO);
             logger.addHandler(fh);
             SimpleFormatter formatter = new SimpleFormatter();
             fh.setFormatter(formatter);
             logger.setUseParentHandlers(disableParentHandlers);
             //fh.close();
             return logger;
         }
    }

