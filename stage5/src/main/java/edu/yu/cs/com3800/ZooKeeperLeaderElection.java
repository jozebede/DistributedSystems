package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 2400;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    LinkedBlockingQueue<Message> incomingMessages;
    ZooKeeperPeerServerImpl myPeerServer;
    long proposedLeader; //long proposedLeaderID
    long proposedEpoch;
    Logger logger;
    Map<Long, ElectionNotification> receivedNotificatoins = new HashMap<Long, ElectionNotification>();

    enum SenderState {
        LOOKING,
        LEADING,
        FOLLOWING
    }


    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {

        this.incomingMessages = incomingMessages;
        this.myPeerServer = (ZooKeeperPeerServerImpl) server;
        this.proposedEpoch = server.getPeerEpoch();
        this.proposedLeader = myPeerServer.getServerId();
        initializeLogging("LeaderElection-on-port-"+myPeerServer.getUdpPort());
        logger.info(" init leader on port:  " + server.getUdpPort());
    }


    public static byte[] buildMsgContent(ElectionNotification notification) {

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES*3+2);
        bb.asLongBuffer().put(notification.getProposedLeaderID()); // leader
        bb.asCharBuffer().put(notification.getState().getChar()); // state char
        bb.asLongBuffer().put(notification.getSenderID()); // sender id
        bb.asLongBuffer().put(notification.getPeerEpoch());// peer epoch
        bb.putLong(notification.getProposedLeaderID()); // leader
        bb.putChar(notification.getState().getChar()); // state char
        bb.putLong(notification.getSenderID()); // sender id
        bb.putLong(notification.getPeerEpoch());// peer epoch
        //bb.rewind();
        byte[] byteArr = new byte[Long.BYTES*3+2];
       // bb.get(byteArr);
        return bb.array();
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        if (received == null){ return null;   }

        received.getMessageContents();
        String[] words = received.toString().split(":|\n");

        String proposedLeaderIDstr = wordFinder(words, "Leader voted for");
        String senderStatestr = wordFinder(words, "Sender server state");
        String senderServerIDstr = wordFinder(words, "Sender server ID");
        String senderPeerEpochstr = wordFinder(words, "Sender peer epoch");

        Long leaderID = Long.parseLong(proposedLeaderIDstr.trim());
        Long senderID = Long.parseLong(senderServerIDstr.trim());
        Long senderEpoch = Long.parseLong(senderPeerEpochstr.trim());
        senderStatestr.trim();
        char c = senderStatestr.charAt(1);
        ZooKeeperPeerServer.ServerState state = ZooKeeperPeerServer.ServerState.getServerState(c);
        return new ElectionNotification(leaderID, state,senderID,senderEpoch);
    }


    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {

        logger.info( this.myPeerServer.getServerId() + "  is looking for leader" );

        //send initial notifications to other peers to get things started
        sendNotifications();

        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState().equals(ZooKeeperPeerServer.ServerState.LOOKING )|| this.myPeerServer.getPeerState().equals(ZooKeeperPeerServer.ServerState.OBSERVER)) {

            //Remove next notification from queue, timing out after 2 times the termination time
            Message msg = null;

            try {
                //if no notifications received  resend notifications to prompt a reply from others
                msg =  this.incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
                if (msg == null){
                  logger.info("  msg was null. trying again... ");
                  msg =incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (msg == null){
                sendNotifications();
                continue; // if we dont have a msg, keep trying...
            }

            ElectionNotification notification =  getNotificationFromMessage(msg);
            Long proposedLeaderID = notification.getProposedLeaderID();
            Long proposedPeerEpoch = notification.getPeerEpoch();
            ZooKeeperPeerServer.ServerState proposedLeaderState = notification.getState();

           if (msg.getSenderPort() == 1030)logger.info("SOMETING IS WROKNG !!! sender is on port 1030");



            // if I get votes for an epoch > mine, I set my own vote epoch to that and proceed from there.
            if (notification.getPeerEpoch() > this.proposedEpoch){
                logger.info( this.myPeerServer.getServerId()+" is updating epoch to " + proposedEpoch);
                this.proposedEpoch = notification.getPeerEpoch();
            }


            //if/when we get a message and it's from a valid server and for a valid server switch on the state of the sender
            if (notification.getPeerEpoch() < this.proposedEpoch || proposedLeaderState.equals(ZooKeeperPeerServer.ServerState.OBSERVER)) {
                logger.info(" receiving message from an invalid server ID " + notification.getSenderID() + " epoch " + notification.getPeerEpoch());
                  sendNotifications();

            } else{


            switch (notification.getState()) {

                case LOOKING: //if the sender is also looking

                    //if the received message has a vote for a leader which supersedes mine,
                    if (supersedesCurrentVote(proposedLeaderID, proposedPeerEpoch)) {
                        // change my vote and tell all my peers what my new vote is.
                        Long lastID = proposedLeader;
                        //  incomingMessages.clear();
                        proposedLeader = notification.getProposedLeaderID();
                        logger.info(myPeerServer.getServerId() + " changed proposed leader from  " + lastID + " to " + proposedLeader);
                        sendNotifications();
                    }

                    //keep track of the votes I received and who I received them from.
                    receivedNotificatoins.put(notification.getSenderID(), notification);


                    //if I have enough votes to declare my currently proposed leader as the leader:
                    if (this.haveEnoughVotes(receivedNotificatoins, this.getCurrentVote())) {

                        //first check if there are any new votes for a higher ranked possible leader before I declare a leader.
                        try {

                            while ((msg = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                ElectionNotification n = getNotificationFromMessage(msg);
                                if (supersedesCurrentVote(n.getProposedLeaderID(), n.getPeerEpoch())) {
                                    this.incomingMessages.put(new Message(buildMsgContent(n)));
                                }

                            }
                        } catch (Exception e) {

                        }


                        //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                        logger.info("we already have enough votes!   " + myPeerServer.getServerId() + " accepting election winner " + proposedLeaderID);
                        acceptElectionWinner(notification);
                        return new Vote(proposedLeader, proposedEpoch);


                    } else {
                        // If so, continue in my election loop
                        continue;
                    }



                //if the sender is following a leader already or thinks it is the leader
                case FOLLOWING:
                case LEADING:

                   if (this.proposedLeader ==myPeerServer.getServerId()) this.proposedLeader = notification.getProposedLeaderID();
                    receivedNotificatoins.put(notification.getSenderID(), notification);

                    //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in,
                    // i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                    if (haveEnoughVotes(receivedNotificatoins, new Vote(this.proposedLeader, this.proposedEpoch))) {
                        //if so, accept the election winner.
                        logger.info(this.myPeerServer.getServerId() +" accepting election -  new leader ID: " + this.proposedLeader + " epoch: " + proposedPeerEpoch);
                        acceptElectionWinner(notification);
                        return new Vote(proposedLeader, proposedEpoch);
                    }

                    //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.

                    //ELSE: if n is from a LATER election epoch
                    //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                    //THEN accept their leader, and update my epoch to be their epoch
                    //ELSE:
                    //keep looping on the election loop.
            }

            }
        }

        return new Vote(proposedLeader,proposedEpoch);
    }

    private void sendNotifications() {


        ElectionNotification n = new ElectionNotification(this.proposedLeader, myPeerServer.getPeerState(),myPeerServer.getServerId(),myPeerServer.getPeerEpoch());
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(n));
        //logger.info("Broadcast sent\n " + n.getSenderID() + " ,   " + n.getState() + " proposed leader " + n.getProposedLeaderID());
        //logger.info("Sending notifications");
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING

        Vote winner = new Vote(n.getProposedLeaderID(),n.getPeerEpoch());

        try {
            myPeerServer.setCurrentLeader(winner);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //clear out the incoming queue before returning
        incomingMessages.clear();
           return winner;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));

    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {

        int count = 0;
        for (ElectionNotification vote : votes.values()){
            if (vote.getProposedLeaderID() == proposal.getProposedLeaderID()){
                count++;
            }
        }

        if (count>= myPeerServer.getQuorumSize()){
            //is the number of votes for the proposal > the size of my peer server’s quorum?
            return true;
        }

        return false;
    }
    protected static String wordFinder(String[] words, String target) {

        for (int i = 0; i < words.length-1; i++) {
            if (words[i].replace("\t","").equals(target)) {
                i++;
                return  words[i];
            }
        }
        return "";
    }

    protected ZooKeeperPeerServer.ServerState getServerState(String str){
        ZooKeeperPeerServer.ServerState serverstate = null;
        if (str.equals("LOOKING")){
            serverstate = ZooKeeperPeerServer.ServerState.LOOKING;
        }
        if (str.equals("LEADING")){
            serverstate = ZooKeeperPeerServer.ServerState.LEADING;
        }
       return serverstate;
    }


    protected void initializeLogging(String fileNamePreface ) {

        Logger logger = Logger.getLogger(fileNamePreface);

        FileHandler fh = null;
        try {
            fh = new FileHandler("./Log/" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(true);

        this.logger = logger;
    }
}
