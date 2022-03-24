package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    //When the leader assigns this node some work to do, this class uses a JavaRunner to do the work,
    // and returns the results back to the leader

    ZooKeeperPeerServerImpl server;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    JavaRunner javaRunner;

    Logger logger;


    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages){

        this.setDaemon(true);
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        setName("RoundRobinLeader-port-" + this.server.getUdpPort());
        this.logger = initializeLogging(JavaRunnerFollower.class.getName() + "-on-port-" + this.server.getUdpPort());
        logger.info("starting worker: " + this.server.getServerId());
        try {
            this.javaRunner = new JavaRunner();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    @Override
    public void run() {

            while (!isInterrupted()) {
                Message msg;
//                JavaRunner javaRunner = null;
//                try {
//                    javaRunner = new JavaRunner();
//                    logger.info("starting java runner !!");
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }


                try {

                msg= incomingMessages.poll(300, TimeUnit.MILLISECONDS);

                if (msg == null){
                    logger.info("msg is null and size is " + incomingMessages.size());
                    msg = incomingMessages.take();
                   // msg = incomingMessages.poll(400, TimeUnit.MILLISECONDS);
                }

                if (msg == null) logger.info("msg is null again ");

                logger.info("we have to do this work  \n \n \n" + msg.toString());


               byte[] contents = msg.getMessageContents();

            //   logger.info("after  content  " + contents.length);
               InputStream in = new ByteArrayInputStream(contents);
               logger.info("InputStream size: " + in.available());


               //in.readAllBytes();

               int senderPort = msg.getSenderPort();
               String senderHost = msg.getSenderHost();
               logger.info("sender port:  " + senderPort);

              // JavaRunner javaRunner = new JavaRunner();
                logger.info("before compile and run");
                String ret = javaRunner.compileAndRun(in);
                logger.info(" RESPONSE  \n" + ret);

                Message response = new Message(Message.MessageType.COMPLETED_WORK, ret.getBytes(),  this.server.getAddress().getHostString(), this.server.getUdpPort(), senderHost, senderPort);
                outgoingMessages.put(response);

            } catch(IOException | ReflectiveOperationException | InterruptedException e){
                    logger.info("catch ! something went wrong "  + e.toString());
                e.printStackTrace();
            }



            }
    }


    public void shutdown() {
        interrupt();
    }

}
