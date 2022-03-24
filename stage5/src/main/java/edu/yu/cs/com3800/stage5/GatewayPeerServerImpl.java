package edu.yu.cs.com3800.stage5;


import edu.yu.cs.com3800.*;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {


    public GatewayPeerServerImpl(int myPort, long peerEpoch, long id, Map<Long, InetSocketAddress> peerIDtoAddress, int observersNum) {
        super(myPort, peerEpoch, id, peerIDtoAddress, observersNum);
        super.setPeerState(ServerState.OBSERVER);

        try {
            super.setCurrentLeader(null);

        } catch (Exception e) {
        }
    }


    @Override
    public Vote getCurrentLeader() {
        return super.getCurrentLeader();
    }
}




