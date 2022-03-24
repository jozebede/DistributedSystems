package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    ZooKeeperPeerServerImpl server;
    JavaRunner javaRunner;
    ServerSocket masterSocket;
    Logger logger;


    public JavaRunnerFollower(ZooKeeperPeerServerImpl server){

        this.setDaemon(true);
        this.server = server;
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

            try {
             //Step 3: Run Java Runner
             String ret = javaRunner.compileAndRun(inpt);
             logger.info(" Compile and Run:\n" + ret);


             // Step 4: send Message back to Leader
             Message responseMsg = new Message(Message.MessageType.COMPLETED_WORK, ret.getBytes(), this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, senderPort,0,false);
             out.write(responseMsg.getNetworkPayload());
             logger.info("Response sent to master!");


            }catch (Exception e){
                logger.info("CATCH!! \n" + e.getMessage() + "\n" + Util.getStackTrace(e));
                Message responseMsg = new Message(Message.MessageType.COMPLETED_WORK, e.getMessage().getBytes(), this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, senderPort,0,true);
                out.write(responseMsg.getNetworkPayload());
                logger.info("Response sent to master!");
            }
         //   masterSocket.close();
        }

        } catch (IOException e) {
            logger.info("Catch!! \n" + e.getMessage() + "\n" + Util.getStackTrace(e) );
        }

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

            this.interrupt();
        }
    }

}


