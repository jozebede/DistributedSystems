package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private  int observersNum;
    private final int myPort;
    private volatile ServerState state;
    private boolean shutdown;

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> heartBets;
    private Long id;
    private long peerEpoch;
    private Vote currentLeader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private LinkedList<Long> failedServersID;
    private LinkedList<InetSocketAddress> failedServersAddress;
    Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private JavaRunnerFollower worker;
    private RoundRobinLeader master;
    private GossipHeartbeat gossip;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, long id, Map<Long, InetSocketAddress> peerIDtoAddress, int observersNum) {

        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.observersNum = observersNum;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.peerIDtoAddress.remove(this.myAddress);
        setPeerState(ServerState.LOOKING);
        this.state = getPeerState();
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.heartBets = new LinkedBlockingQueue<>();
        this.failedServersID = new LinkedList<>();
        this.failedServersAddress = new LinkedList<>();
        this.currentLeader = new Vote(this.id,this.peerEpoch);
        this.logger = initializeLogging("PeerlogFile on port " + this.myPort);
        logger.log(Level.INFO, "New Peer server: " + this.getServerId());


        try {
        //step 1: create and run thread that sends broadcast messages
        senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        senderWorker.start();
        logger.info(" new senderWorker = UDPMessageSender on port " + this.getUdpPort());

        //step 2: create and run thread that listens for messages sent to this server
        receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.heartBets, this.myAddress, this.myPort, this);
        receiverWorker.start();
        logger.info(" new ReceiverWorker = UDPMessageReceiver on port " + this.getUdpPort());
        } catch (IOException e) {
            logger.info("warning - catch! creating sender/receiver Threads \n " + Util.getStackTrace(e));
        }

        this.gossip = new GossipHeartbeat(heartBets, outgoingMessages, peerIDtoAddress, this);
        gossip.start();

    }


    @Override
    public void shutdown() {
        logger.info("shutdown  " + this.id);

        this.gossip.shutdown();
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();



        if (this.worker != null) {
            this.worker.shutdown();
            this.worker.interrupt();
        }
       if (this.master != null)this.master.shutdown();
        this.interrupt();

    }


    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
        if (v ==null|| getPeerState().equals(ServerState.OBSERVER)) return;
        if (v.getProposedLeaderID() == this.id) {
            state = ServerState.LEADING;
        }else {
            state = ServerState.FOLLOWING;
        }
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for (InetSocketAddress peer : peerIDtoAddress.values()){
                sendMessage(type,messageContents,peer);
        }
    }


    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        if (newState == null) throw new IllegalArgumentException("newState is null");
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() { return this.myPort; }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        //assume all servers are live, remove observers from count
        return peerIDtoAddress.size() - this.observersNum;
    }

    @Override
    public void reportFailedPeer(long peerID) {
        logger.info("Failed peer! ID: " + peerID);


        // if the failed node is the leader:
        if (currentLeader.getProposedLeaderID() == peerID){
            // if the leader die, we need to run a new LeaderElection

            currentLeader=null;
            peerEpoch++;
            if (getPeerState().equals(ServerState.OBSERVER)){
            } else{
                master = null;
                //worker = null;
                logger.info(this.getServerId() + " switching from " + getPeerState() +" to LOOKING");
                System.out.println(this.getServerId() + " switching from " + getPeerState() +" to LOOKING");
                setPeerState(ServerState.LOOKING);
            }
        }

        failedServersID.add(peerID);
        failedServersAddress.add(peerIDtoAddress.get(peerID));
        if (master!=null)this.master.addFaultyNode(peerIDtoAddress.get(peerID));
        if (worker!=null)this.worker.addFaultyNode(peerIDtoAddress.get(peerID));
        peerIDtoAddress.remove(peerID);

    }

    @Override
    public boolean isPeerDead(long peerID) {
        return failedServersID.contains(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return failedServersAddress.contains(address);
    }

    @Override
    public void run() {

        try {

            // main server loop
            while (!this.shutdown) {

                // check if any sever is down

                switch (getPeerState()) {

                    case OBSERVER:
                        //start leader election as OBSERVER.
                        if (currentLeader!=null)continue;
                        logger.info("Observer looking for its leader");
                       // setCurrentLeader(null);
                        ZooKeeperLeaderElection observerElection = new ZooKeeperLeaderElection(this, this.incomingMessages);
                        Vote observerVote = observerElection.lookForLeader();
                        setCurrentLeader(observerVote);
                        logger.info("Observer have a new leader: " + observerVote.getProposedLeaderID());
                        break;


                    case LOOKING:
                        //start leader election, set leader as the election winner
                        logger.info("starting leader election");

                        ZooKeeperLeaderElection elect = new ZooKeeperLeaderElection(this, incomingMessages);
                        Vote leader = elect.lookForLeader();
                        setCurrentLeader(leader);
                        logger.info("we have a leder! " + leader.toString());
                        break;


                    case LEADING:
                        if (master == null) {
                            if (worker!=null) worker.shutdown(); // if this node was a follower before
                            this.master = new RoundRobinLeader(this, peerIDtoAddress);
                            this.master.start();
                            logger.info("new master ID: " + this.id); }
                        break;


                    case FOLLOWING:
                        if (worker == null ){
                            this.worker = new JavaRunnerFollower(this);
                            this.worker.start();
                            logger.info("new worker ID:  " + this.id);}
                        break;

                }
            }
        } catch (Exception e) {
            logger.info(this.id + "  ERROR in run(): \n   "  + Util.getStackTrace(e));
        }

    } // run()
}