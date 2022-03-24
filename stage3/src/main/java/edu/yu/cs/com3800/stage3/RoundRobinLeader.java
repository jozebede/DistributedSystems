package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.UDPMessageReceiver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {

    Logger logger;
    int myPort;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    Map<Long, InetSocketAddress> peerIDtoAddress;
    ZooKeeperPeerServerImpl server;
    ArrayList<InetSocketAddress> workers;

    /*
    when a client request comes in, the master will simply assign the client request to the next worker in the list of workers. When the master gets
    to the end of the list of workers, it goes back to the beginning of the list and proceeds the same exact way. You should think of
    the list of workers as a circularly linked list which the master is looping through forever – the master assigns a request to the
    current worker and then advances to the next worker in the list.

    The master thread MUST NOT synchronously wait (a.k.a. block) for a worker to complete a task; all work in the cluster is done
    asynchronously (otherwise, we would gain nothing by having multiple servers.) The master sends work to a worker via a UDP
    message and receives results via a UDP message; there is no synchronous connection or blocking.
     */

   public RoundRobinLeader(ZooKeeperPeerServerImpl server, Map<Long, InetSocketAddress> peerIDtoAddress, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages){
       this.incomingMessages = incomingMessages;
       this.outgoingMessages = outgoingMessages;
       this.peerIDtoAddress = peerIDtoAddress;
       this.myPort = server.getUdpPort();
       this.server = server;
       this.setDaemon(true);
       setName("RoundRobinLeader-port-" + this.myPort);
       this.logger = initializeLogging(RoundRobinLeader.class.getName() + "-on-port-" + this.myPort);
       logger.info("starting master worker. server id  " + this.server.getServerId());
      this.workers = new ArrayList<>(this.peerIDtoAddress.values());
   }

    // It assigns work it to followers
    //on a round-robin basis, gets the results back from the followers, and sends the responses to the one who requested the
    //work, i.e. the client.

    @Override
    public void run() {



           int count = -1; // keep track of available workers.
           int clientPort = 0;
           String clientHost = null;

           while (!this.isInterrupted()) {

               if (count > workers.size()-1) count = -1; // reset loop of available workers

               try {

                   Message msg;
                   // get the msg from client
                   msg = incomingMessages.poll(300, TimeUnit.MILLISECONDS);
                   if (msg == null){
                       logger.info("msg is null" );
                        msg = incomingMessages.take();
                        if (msg != null & msg.getMessageType().equals(Message.MessageType.COMPLETED_WORK))logger.info(" \n \n \n WE HAVE A COMPLETED WOORKKK \n \n \n");
                       //msg = incomingMessages.poll(300, TimeUnit.MILLISECONDS);
                   }

                   if (msg != null && msg.getMessageType().equals(Message.MessageType.WORK)) {

                       logger.info("master received message" + msg.toString());

                       // get client host and port
                       clientPort = msg.getSenderPort();

                        clientHost = msg.getSenderHost();
                       String code = new String(msg.getMessageContents(), StandardCharsets.UTF_8);

                       // send WORK msg to available worker
                       InetSocketAddress worker = workers.get(++count);

                       Message sending = new Message(Message.MessageType.WORK, code.getBytes(), this.server.getAddress().getHostString(), this.myPort, worker.getHostString(), worker.getPort());
                       this.outgoingMessages.put(sending);

                   } else if(msg != null ){ //&& msg.getMessageType().equals(Message.MessageType.COMPLETED_WORK)) {

                       logger.info("completed work received! ");

                       // receive WORK msg from worker and send it back to the client
                       byte[] content = msg.getMessageContents();
                       Message answer = new Message(Message.MessageType.COMPLETED_WORK, content, this.server.getAddress().getHostString(), this.myPort, clientHost, clientPort);
                       this.outgoingMessages.put(answer);

                       String s = new String(content, StandardCharsets.UTF_8);
                       logger.info(" \n completed work sent!!   \n " + s);

                       }



               } catch (InterruptedException e) {
                   e.printStackTrace();
               }

           }


        }


    public void shutdown() {
        interrupt();
    }

}
