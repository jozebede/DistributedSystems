package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    ZooKeeperPeerServerImpl server;
    JavaRunner javaRunner;
    ServerSocket masterSocket;
    Logger logger;
    HashMap<Long,String> responseCache;
    private LinkedList<Integer> failedServersPort;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server){

        this.setDaemon(true);
        this.failedServersPort = new LinkedList<>();
        this.server = server;
        responseCache = new HashMap<>();
        setName("RoundRobinLeader-port-" + this.server.getUdpPort());
        this.logger = initializeLogging(JavaRunnerFollower.class.getName() + "-on-port-" + this.server.getUdpPort());
        logger.info("starting worker ID: " + this.server.getServerId());
        try {
            this.javaRunner = new JavaRunner();
        } catch (IOException e) {
            e.printStackTrace();
        }
   }


    @Override
    public void run() {


        try{
            int connectionPort = this.server.getUdpPort()+2;
            logger.info("Establishing TCP Connection on port " + connectionPort);
            this.masterSocket = new ServerSocket(connectionPort);

        while (!isInterrupted()) {

            Message msg;
            byte[] receiveBuf = new byte[400];
            Socket connection = masterSocket.accept();
            InputStream in = connection.getInputStream();
            OutputStream out = connection.getOutputStream();

            // Step 1: receive Message from Leader
            in.read(receiveBuf);
            msg = new Message(receiveBuf);
            logger.info("Received Work from Master: \n" + msg.toString());

           // Step 2: convert Java code to InputStream
            String s = new String(msg.getMessageContents(), StandardCharsets.UTF_8);
            InputStream inpt = new ByteArrayInputStream(s.getBytes());
            int senderPort = msg.getSenderPort();
            String senderHost = msg.getSenderHost();
            Long requestID = msg.getRequestID();

            try {
             //Step 3: Run Java Runner
             String ret = javaRunner.compileAndRun(inpt);
             logger.info(" Compile and Run:\n" + ret);


             // Step 4: check if leader is alive
                if (failedServersPort.contains(senderPort-2)){
                    logger.info("leader on port " + senderPort + " is dead");
                    responseCache.put(requestID,ret); //  followers: queue responses from completed work.

                    while (server.getCurrentLeader() == null ){
                        // wait until we have a leader
                    }
                    long newLeaderID = server.getCurrentLeader().getProposedLeaderID();
                    int newSenderPort = server.getPeerByID(newLeaderID).getPort() +2;
                    logger.info(" sending complete work to new leader " + newLeaderID + " on port " + newSenderPort );

                    Message responseMsg = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, ret.getBytes(), this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, newSenderPort,requestID,false);
                    out.write(responseMsg.getNetworkPayload());
                   // logger.info("Response sent to master " + newLeaderID + " on port " + newSenderPort);

                    // remove from queue
                    responseCache.remove(requestID);
                    continue;
                }else{
                    logger.info("node on port " + senderPort + " is alive..");
                }


            // Setep 5: send Message back to Leader
             Message responseMsg = new Message(Message.MessageType.COMPLETED_WORK, ret.getBytes(), this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, senderPort,requestID,false);
             out.write(responseMsg.getNetworkPayload());
             logger.info("Response sent to master!");


            }catch (Exception e){
                logger.info("CATCH!! \n" + e.getMessage() + "\n" + Util.getStackTrace(e));
                Message responseMsg = new Message(Message.MessageType.COMPLETED_WORK, e.getMessage().getBytes(), this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, senderPort,requestID,true);
                out.write(responseMsg.getNetworkPayload());
                logger.info("Response sent to master!");
            }
         //   masterSocket.close();
        }

        } catch (IOException e) {
            logger.info("Catch!! \n" + e.getMessage() + "\n" + Util.getStackTrace(e) );
        }

    }

    public void addFaultyNode(InetSocketAddress address) {
        logger.info("worker adding Faulty node to the List.  port " + address.getPort());
        this.failedServersPort.add(address.getPort());
        //workers.remove(address);
    }


    public void shutdown() {
        logger.info("Shutdown JavaRunnerfollower");
        if (masterSocket != null) {
            try {

                masterSocket.close();
               //  sleep(2000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            javaRunner = null;
            this.interrupt();
        }
    }

}


