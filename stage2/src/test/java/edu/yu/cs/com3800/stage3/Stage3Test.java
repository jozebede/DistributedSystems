package edu.yu.cs.com3800.stage3;


import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import edu.yu.cs.com3800.SimpleServer;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.stage2.ZooKeeperPeerServerImpl;
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


public class Stage3Test {
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


    @Test
    void testing1() {

        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
            servers.add(server);
            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
        }


        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        for (ZooKeeperPeerServerImpl server : servers) {
            Vote proposedLeaderID = server.getCurrentLeader();

            long actualLeader = 8;
            long actualEpoch = 0;

            assertEquals(actualLeader, proposedLeaderID.getProposedLeaderID());
            assertEquals(actualEpoch, proposedLeaderID.getPeerEpoch());

        }

    }
}
