package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage3.JavaRunnerFollower;
import edu.yu.cs.com3800.stage3.RoundRobinLeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private long peerEpoch;
    private Vote currentLeader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private JavaRunnerFollower worker;
    private RoundRobinLeader master;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        //code here...
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.peerIDtoAddress.remove(this.myAddress);  // sale en el example. no entiendo pq remove?
        this.state = ServerState.LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.currentLeader = new Vote(this.id,this.peerEpoch);
        this.logger = initializeLogging("PeerlogFile");
        logger.info("New Peer server: " + this.getServerId());
    }


    @Override
    public void shutdown() {
        logger.info("shutdown  " + this.id);
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.worker.shutdown();
        this.master.shutdown();
    }


    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
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

        String str = new String(messageContents, StandardCharsets.UTF_8);
        //  logger.info(this.id +" sending broadcast: " + str);

        for (InetSocketAddress peer : peerIDtoAddress.values()){
            try{

                Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, peer.getHostString(), peer.getPort());
                this.outgoingMessages.offer(msg);


            } catch(Exception e){
                logger.info("catch! - ERROR sending Broadcast  " + e.toString());
            }
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
        //assume all servers are live
        return peerIDtoAddress.size(); // revisar
    }

    @Override
    public void run() {


        try {
            //step 1: create and run thread that sends broadcast messages
            senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
            senderWorker.start();
            logger.info(" new senderWorker = UDPMessageSender " + this.id);
            //step 2: create and run thread that listens for messages sent to this server
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
            receiverWorker.start();
            logger.info("ReceiverWorker = UDPMessageReceiver " + this.id);

            //step 3: main server loop
            while (!this.shutdown) {

                switch (getPeerState()) {


                    case LOOKING:
                        //  logger.info("server is LOOKING");


                        //start leader election, set leader to the election winner
                        ZooKeeperLeaderElection elect = new ZooKeeperLeaderElection(this, incomingMessages);
                        Vote leader = elect.lookForLeader();
                        setCurrentLeader(leader);
                        logger.info("we have a leder! " + leader.toString());

                        break;


                    case LEADING:
                        if (master == null) {
                            this.master = new RoundRobinLeader(this, peerIDtoAddress, incomingMessages, this.outgoingMessages);
                            this.master.start();
                            logger.info("new master:  " + this.id);
                        }

                        break;

                    case FOLLOWING:
                            if (worker == null ){
                        this.worker = new JavaRunnerFollower(this, this.incomingMessages, this.outgoingMessages);
                        this.worker.start();
                        logger.info("new worker:  " + this.id);}

                        break;



                }
            }
        } catch (Exception e) {
            logger.info(this.id + "  ERROR in run():  " + e.toString());
        }

    }

    protected void setEpochTime(){
        SimpleDateFormat crunchifyFormat = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");

        Date today = Calendar.getInstance().getTime();
        String currentTime = crunchifyFormat.format(today);
        Date date = null;
        try {
            date = crunchifyFormat.parse(currentTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        this.peerEpoch= date.getTime();
    }

}