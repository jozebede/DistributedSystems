package edu.yu.cs.com3800.stage3;


import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class Stage3Test {

    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
   // private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    private int[] ports = {8010, 8020};
    private int leaderPort = this.ports[this.ports.length - 1];
    private int myPort = 9999;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<ZooKeeperPeerServer> servers;
    protected int leaderId = 7;


    @Test
    public void Stage3PeerServerDemo() throws Exception {
            //step 1: create sender & sending queue
            this.outgoingMessages = new LinkedBlockingQueue<>();
            UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages, myPort); // server udp port
            //step 2: create servers
            createServers();
            //step2.1: wait for servers to get started
            try {
                Thread.sleep(3000);
            }
            catch (Exception e) {
            }

            // check current leaders


//        for (ZooKeeperPeerServer server : this.servers){
//
//            assertTrue(server.getCurrentLeader().getProposedLeaderID() == leaderId);
//
//        }


            //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
            for (int i = 0; i < this.ports.length; i++) {
                String code = this.validClass.replace("world!", "world! from code version " + i);
                sendMessage(code);
            }
            Util.startAsDaemon(sender, "Sender thread");
            this.incomingMessages = new LinkedBlockingQueue<>();

            UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, null); //   ZooKeeperPeerServer peerServer
            Util.startAsDaemon(receiver, "Receiver thread");
            //step 4: validate responses from leader

            printResponses();

            //step 5: stop servers
            stopServers();
        }


        private void stopServers() {
            for (ZooKeeperPeerServer server : this.servers) {
                server.shutdown();
            }
        }

        private void printResponses() throws Exception {
            String completeResponse = "";
            for (int i = 0; i < this.ports.length; i++) {
                Message msg = this.incomingMessages.take();
                String response = new String(msg.getMessageContents());
                completeResponse += "Response #" + i + ":\n" + response + "\n";
            }
            System.out.println(completeResponse);
        }

        private void sendMessage(String code) throws InterruptedException {
            Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", this.leaderPort);
            this.outgoingMessages.put(msg);
        }

        private void createServers() {
            //create IDs and addresses
            HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
            for (int i = 0; i < this.ports.length; i++) {
                peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
            }
            //create servers
            this.servers = new ArrayList<>(3);
            for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                ZooKeeperPeerServer server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                this.servers.add(server);
                new Thread((Runnable) server, "Server on port " + server.getAddress().getPort()).start();
            }
        }




//
//    @Test
//    void test00(){
//
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
//        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
//        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
//        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
//        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
//        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
//
//        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//
//        ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(8010, 0, 1L, map);
//        new Thread(server, "Server on port " + server.getAddress().getPort()).start();
//
//        ZooKeeperPeerServerImpl server2 = new ZooKeeperPeerServerImpl(8020, 0, 2L, map);
//        new Thread(server2, "Server on port " + server2.getAddress().getPort()).start();
//
//        ZooKeeperPeerServerImpl server3 = new ZooKeeperPeerServerImpl(8030, 0, 3L, map);
//        new Thread(server3, "Server on port " + server3.getAddress().getPort()).start();
//
//        ZooKeeperPeerServerImpl server4 = new ZooKeeperPeerServerImpl(8040, 0, 4L, map);
//        new Thread(server4, "Server on port " + server4.getAddress().getPort()).start();
//
//        ZooKeeperPeerServerImpl server5 = new ZooKeeperPeerServerImpl(8050, 0, 5L, map);
//        new Thread(server5, "Server on port " + server5.getAddress().getPort()).start();
//
//
//        try {
//            Thread.sleep(1000);
//        } catch (Exception e) {
//        }
//
//        for (int  i =0; i < 7; i++) {
//            Vote proposedLeaderID = server.getCurrentLeader();
//
//            long actualLeader = 8;
//            long actualEpoch = 0;
//
//            assertEquals(actualLeader, proposedLeaderID.getProposedLeaderID());
//            assertEquals(actualEpoch, proposedLeaderID.getPeerEpoch());
//
//
//    }

//
//    @Test
//    void testing1() {
//
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
//        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
//        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
//        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
//        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
//        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
//        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
//        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
//        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
//
//        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>(3);
//        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
//            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//            map.remove(entry.getKey());
//            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
//            servers.add(server);
//            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
//        }
//
//
//        try {
//            Thread.sleep(1000);
//        } catch (Exception e) {
//        }
//
//        for (ZooKeeperPeerServerImpl server : servers) {
//            Vote proposedLeaderID = server.getCurrentLeader();
//
//            long actualLeader = 8;
//            long actualEpoch = 0;
//
//            assertEquals(actualLeader, proposedLeaderID.getProposedLeaderID());
//            assertEquals(actualEpoch, proposedLeaderID.getPeerEpoch());
//
//        }
//
//    }
}
