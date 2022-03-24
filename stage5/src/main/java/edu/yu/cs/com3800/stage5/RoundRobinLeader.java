package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RoundRobinLeader extends Thread implements LoggingServer {

    Logger logger;
    int myPort;
    boolean removedGateway = false;
    static int gatewayPort;
    static String gatewayHost = "localhost";
    Map<Long, InetSocketAddress> peerIDtoAddress;
    static ZooKeeperPeerServerImpl server;
    ArrayList<InetSocketAddress> workers;
    ServerSocket socket = null;
    workersManager workerThread;
    private LinkedList<InetSocketAddress> failedServersAddress;
   //static Map<Integer, Stack<Message>> messagesTrack; // keep track of which worker receive messages // todo - make it atomic
    HashMap<Long, Message> unsent =  new HashMap<>();
    HashMap<Long, byte[]> MessageCache =  new HashMap<Long, byte[]>(); // cache request ID with Message response


    public RoundRobinLeader(ZooKeeperPeerServerImpl server, Map<Long, InetSocketAddress> peerIDtoAddress) {

        this.peerIDtoAddress = peerIDtoAddress;
        this.myPort = server.getUdpPort();
        this.server = server;
        this.failedServersAddress  = new LinkedList<>();
        this.setDaemon(true);
        //this.messagesTrack = new HashMap<>();
        setName("RoundRobinLeader-port-" + this.myPort);
        this.logger = initializeLogging(RoundRobinLeader.class.getName() + "-on-port-" + this.myPort);
        logger.info("starting master worker. server id  " + this.server.getServerId());
        this.workers = new ArrayList<>(this.peerIDtoAddress.values());
//        for (InetSocketAddress address: peerIDtoAddress.values()){
//            messagesTrack.put(address.getPort(),new Stack<>());
//        }
    }


    @Override
    public void run() {

        try {

            int count = -1; // keep track of available workers.
            int con = myPort+2; // TCP port
            logger.info("receiving TCP connection on port - " + con);
            socket = new ServerSocket(con);  //Get the msg from Gateway
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);


            while (!this.isInterrupted()) {

                if (count >= workers.size() - 1) count = -1; // reset loop of available workers

                Message msg = null;
                byte[] receiveBuf = new byte[400];  // Receiver buffer
                Socket gatewaySocket = socket.accept();     // TCP connection with Gateway
                InputStream gatewayIn = gatewaySocket.getInputStream();
                OutputStream gatewayOut = gatewaySocket.getOutputStream();



                    gatewayIn.read(receiveBuf);
                    msg = new Message(receiveBuf);
                    logger.info("Received Message from Gateway: \n" + msg.toString());


                //Step 1: check if we have this request on cache.
                if(MessageCache.containsKey(msg.getRequestID())){
                    // Send cache response back to Gateway.
                    Message answer = new Message(Message.MessageType.COMPLETED_WORK,  MessageCache.get(msg.getRequestID()), msg.getSenderHost(), msg.getSenderPort(), RoundRobinLeader.gatewayHost, RoundRobinLeader.gatewayPort, msg.getRequestID(), false);
                    gatewayOut.write(answer.getNetworkPayload());
                    logger.info("response sent to Gateway:\n" + answer.toString());
                    continue;
                }



                // Step 2:  remove gateway from the list of available workers
                removeGateway(msg);

                // Step 3: find next available worker.
                InetSocketAddress worker = workers.get(++count);


                // check if the worker is dead
                if (this.failedServersAddress.contains(worker)){
                    logger.info("found a dead worker! on port " + worker.getPort());
                    workers.remove(worker); // delete from the list
                    if (count >= workers.size() - 1) count = -1; // reset loop of available workers
                     worker = workers.get(++count);
              }else {
                    //logger.info(" server on port " + worker.getPort() + " is alive");
                }

                int p = worker.getPort()+2;
                logger.info("Sending work to JavaRunnerWorker in TCP port: " + p);
                Message sending = new Message(Message.MessageType.WORK, msg.getMessageContents(), this.server.getAddress().getHostString(), this.myPort+2, worker.getHostString(), p, msg.getRequestID());

                // Start new worker manager Thread. it will send work to the worker and wait for the response.
//              Stack<Message> messagesQueue =  messagesTrack.get(worker.getPort());
//              logger.info("adding message to queue of size: " + messagesQueue.size() +"\n");
//              messagesQueue.push(sending);

                this.workerThread = new workersManager(sending, worker, gatewayOut, server, MessageCache);
                logger.info("Creating Work Thread");
                executor.execute(workerThread);
        }

        } catch (IOException e) {
            logger.info("Catch! \n" + Util.getStackTrace(e));
            e.printStackTrace();
        }
    } // run()





    public void shutdown() {
        logger.info("Shutdown RoundRobinLeader");
        try {
         if (this.socket != null)this.socket.close();
           if (workerThread != null)workerThread.shutdown();
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

    public void addFaultyNode(InetSocketAddress address) {
        logger.info("master adding Faulty node to the List.  port " + address.getPort());
        this.failedServersAddress.add(address);
        workers.remove(address);
    }
}

class workersManager extends Thread{

       Message msg;
       InetSocketAddress worker;
       OutputStream gatewayOut;
       Logger logger;
       ZooKeeperPeerServer server;
       HashMap<Long, byte[]> MessageCache;
        Socket workerSocket;

       workersManager(Message msg, InetSocketAddress worker, OutputStream  gatewayOut, ZooKeeperPeerServer server, HashMap<Long, byte[]> MessageCache){
         this.msg = msg;
         this.server = server;
         this.MessageCache = MessageCache;
         this.worker = worker;
         this.setDaemon(true);
         this.setName("WorkerManager for worker on port" + worker.getPort());
         this.gatewayOut = gatewayOut;
         this.logger = initializeLogging("WorkerManagerThread-on-port-"+ worker.getPort()); // worker.getPort());
         logger.info("Starting WorkerManagerThread...");

       }

    public void shutdown() {
        try {
          if (this.workerSocket!=null) this.workerSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.interrupt();

    }



        @Override
        public void run() {

            try {


                int workerPort = worker.getPort() + 2;
                logger.info("trying to connect  TCP   with Server on port " + workerPort);

                if (server.isPeerDead(workerPort)) {
                    logger.info("Worker on port" + workerPort + " is dead (once) ");
                    //unsent.add(msg);
                }

                this.workerSocket = new Socket(worker.getHostName(), workerPort);
                OutputStream workerOut = workerSocket.getOutputStream();
                InputStream workerIn = workerSocket.getInputStream();
                logger.info("Establishing TCP  connection with Server on port " + workerPort);

                // Step 1: Send TCP message to JavaRunnerFollower.
                byte[] data = msg.getNetworkPayload();
                workerOut.write(data);
                logger.info("request ID " + msg.getRequestID() + "  sent to JavaRunnerFollower on port " + workerPort);


                // Step 2: Receive response from JavaRunnerFollower
//            logger.info("receiving response from worker");
                byte[] receiveBuf = new byte[400];
                workerIn.read(receiveBuf);
                Message workerResponse = new Message(receiveBuf);
                logger.info("Received response from worker on TCP port:  " + workerPort + "\n" + workerResponse.toString());

                if (workerResponse.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                    MessageCache.put(workerResponse.getRequestID(), workerResponse.getMessageContents());

                } else {


                    // check if worker is dead?
                    if (server.isPeerDead(workerPort)) {
                        logger.info("Worker on port" + workerPort + " is dead ");
                        //unsent.put(msg.getRequestID(),msg);
                        // todo check this
                    } else {


                        //Step 3: Send response back to Gateway.
                        Message answer = new Message(Message.MessageType.COMPLETED_WORK, workerResponse.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(), RoundRobinLeader.gatewayHost, RoundRobinLeader.gatewayPort, workerResponse.getRequestID(), workerResponse.getErrorOccurred());
                        gatewayOut.write(answer.getNetworkPayload());
                        logger.info("response sent to Gateway:\n" + answer.toString());
                        workerSocket.close();

                    }
                }

                } catch(IOException e){
                    logger.info("CATCH \n " + Util.getStackTrace(e));

                    e.printStackTrace();
                }

        }


    protected Logger initializeLogging(String fileNamePreface ) {

//        if (logger != null)return this.logger;

        boolean disableParentHandlers = true;
        Logger logger = Logger.getLogger(fileNamePreface);
        //logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
            fileNamePreface.replace("edu.yu.cs.com3800", "");
            fh = new FileHandler("./Log/LogFile_" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // fh.setLevel(Level.INFO);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(disableParentHandlers);
        //fh.close();

        return logger;

    }

    }

