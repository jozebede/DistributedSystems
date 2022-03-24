package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GossipHeartbeat extends Thread implements LoggingServer {

    protected Map<Long, InetSocketAddress> peerIDtoAddress;
    LinkedBlockingQueue<Message> outgoingMessages;
    LinkedBlockingQueue<Message> heartBeats;
    protected ZooKeeperPeerServerImpl myPeerServer;
    //Hashtable<Long,Integer> failureDetection = new Hashtable<>();
    int[][] failureDetection;
    protected int myPort;
    protected String myHost;
    static final int GOSSIP = 2000;
    static final int FAIL = GOSSIP * 9;  // - fix this numbers!
    static final int CLEANUP = FAIL * 2;
    int[]randomServerID;
    Random random;
    Logger logger;
    Logger messageListLogger;
    int heartBeatCounter;
    long startTime;
    long elapsedSeconds;
    HttpServer server;
   public static  ArrayList<String> receivedMessages;



    public GossipHeartbeat( LinkedBlockingQueue<Message> heartBeats,  LinkedBlockingQueue<Message> outgoingMessages, Map<Long, InetSocketAddress> peerIDtoAddress, ZooKeeperPeerServerImpl myPeerServer){
        this.heartBeats = heartBeats;
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myPeerServer = myPeerServer;
        this.myPort = myPeerServer.getUdpPort();
        this.receivedMessages = new ArrayList<>();
        this.myHost = "localhost";
        this.messageListLogger = initializeHeartbeatLog("MessageListLogger-on-port-"+myPort);
        this.randomServerID =  new int[peerIDtoAddress.size()];
        this.random = new Random();
        this.heartBeatCounter = -1;
        this.startTime = System.currentTimeMillis();

//        this.elapsedTime = System.currentTimeMillis() - startTime;
//        this.elapsedSeconds = elapsedTime / 1000;






        this.logger = initializeLogging("LogFile_GossipHeartbeats-on-port-"+myPort);
        logger.info("Initializing GossipHeartbeats on server ID " + myPeerServer.getServerId());

        generateRandomArray(); //  fills the array of available servers. "failureDetection[]"
        initFailureDetection(); // init the table of ID,Heartbeat


    }

    //todo-   IMPORTANT
    /**
     Every node must:

    ▪keep a list of all the Gossip messages it
    receives from other nodes. Each element in the
    list should include the machine it received the
    message from, the message contents, as well as the
    time it was received.

    ▪Provide an http accessible ser vice to retrieve
    that list AND log it out to a human-readable log
    file which is separate from the rest of its log

     */

    @Override
    public void run() {

        // create HTTPServer
        try {
            int serverport = myPort+4;
            this.server = HttpServer.create(new InetSocketAddress(serverport), 8);
            server.createContext("/getLogs", new LogHandle());
            logger.info("creating Message list HTTP accesible on port  " + serverport);
            server.setExecutor(null);
            this.server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }


        while (!this.isInterrupted()) {
            //Every T_gossip seconds


            // Step 1:  increment heartbeat counter
            heartBeatCounter++;
            updateMyHeartbeat();

            //Step 2:  gossip heartbeat data (pick a random node and send it the data I have about all the nodes)
            int randomIndex = random.nextInt(randomServerID.length);
           // logger.info("random index " + randomIndex);
            int randomID = randomServerID[randomIndex];
            InetSocketAddress randomServer = peerIDtoAddress.get(Long.valueOf(randomID));
            int randomPort = randomServer.getPort();

            byte[] content = integersToBytes(this.failureDetection, myPeerServer.getServerId());
            Message heartbeat = new Message(Message.MessageType.GOSSIP, content, myHost, myPort, "localhost", randomPort);
            this.outgoingMessages.offer(heartbeat);
           // logger.info("Sending HeartBeat to server ID: " + randomID + " on port: " + randomPort);
            checkFailures();

            //Step 3: Receive Gossip Message
            Message beat = null;
            try {
                beat = heartBeats.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (beat == null)  continue;



            //   Step 4:  construct message and merge  with local data
            //  lower values replaced with higher values, a-la vector clocks
           int[][] receivedTable =  constructTableFromMessage(beat.getMessageContents());



            // Step 5: check if some node have fail
             //checkFailures();


            try {
                sleep(GOSSIP);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }//run


    public void shutdown() {

        logger.info("Shutdown GossipHeartbeat on node ID "); //+ this.myPeerServer.getServerId());
        this.outgoingMessages = null;
        this.interrupt();
    }

    protected void updateTable(int[][] newTable, long senderID){
    //  logTable();

        long current = System.currentTimeMillis() - startTime;
        int currentTime = (int) current/100;

        for (int i = 0; i < newTable.length;i++){

            if(failureDetection[i][1]<newTable[i][1]){
                //insert this node’s ID here]: updated [ser verID]’s heartbeat sequence to [sequence number] based on message from [source server ID]
                // at node time [this node’s clock time]”
                logger.info(myPeerServer.getServerId() + " updated " + failureDetection[i][0] + "’s heartbeat sequence to " +  newTable[i][1] +  " based on message from " + senderID +" at node time " + currentTime);
                // todo - revisar si el heartbeat es igual
               // logger.info("Updating Table\nNode ID: " + failureDetection[i][0] + "\nOld Heartbeat count: " +  failureDetection[i][1] + " new Heartbeat count: " + newTable[i][1] + "\nCurrent time: " + currentTime );
                failureDetection[i][1] = newTable[i][1];  // update heartbeat count
                failureDetection[i][2] = currentTime; // update received time
            }

        }

    }

    // iterates the table to see if there is a timeout
    // return the ID of the faulty node or null if there is none.
    public long checkFailures(){

        long current = System.currentTimeMillis() - startTime;
        int currentTime = (int)current/100;
        //logTable();

        for (int i = 0; i < failureDetection.length;i++){
                int fails =  FAIL/100;
          //  logger.info("current time: " + currentTime + " Fail time: " + fails);

          if(currentTime- failureDetection[i][2]  >= CLEANUP/100) {
                logger.info("cleanup ");
                deleteFromTable(i);

          } else if(currentTime- failureDetection[i][2]  >= FAIL/100){

              logger.info( myPeerServer.getServerId() +" no heartbeat from server " + failureDetection[i][0] );
              System.out.println(myPeerServer.getServerId() +" no heartbeat from server " + failureDetection[i][0] );

              long failedID = failureDetection[i][0];
              deleteFromRandomArray(failedID);
              this.myPeerServer.reportFailedPeer(failedID);

              return failedID;
          }

        }

      return -1;
    }

    protected void deleteFromRandomArray(long id) {
       // logger.info("delete from random array ");
        int[] newRandom = new int[randomServerID.length-1];
        int newIndex =0;

        for (int i = 0; i<randomServerID.length;i++) {
            if (randomServerID[i] != id) {
                newRandom[newIndex] = randomServerID[i];
                newIndex++;
            }
        }
        //logger.info("new random array " + newRandom[0] + "  , " + newRandom[1] );
        randomServerID = newRandom;
    }


    // delete the failed node from our membership table
    protected void deleteFromTable(int index){
        int[][] newTable = new int[failureDetection.length-1][3];
        int newIndex =0;

      //  logger.info("deleting from table index " + index);
        for (int i = 0; i<failureDetection.length;i++){
            if (i!=index){
        //        logger.info("im here on index " + i);
                newTable[newIndex][0] = failureDetection[i][0];
                newTable[newIndex][1] = failureDetection[i][1];
                newTable[newIndex][2] = failureDetection[i][2];
                newIndex++;
            }
        }

        logSmallTable(newTable);
        this.failureDetection = newTable; // update table
       // logTable();
    }


    protected int[][] constructTableFromMessage (byte[] content) {

     //   logger.info("Constructing Table from Message");

        int[][] temporal = new int[peerIDtoAddress.size()+1][3];

        ByteBuffer msgBytes = ByteBuffer.wrap(content);
        int contentwithoutlong = content.length-8;
        int contentSize = contentwithoutlong/(3*4);

        for (int i = 0; i<contentSize; i++){

            int id = msgBytes.getInt();
            int heartBeat = msgBytes.getInt();
            int localTime = msgBytes.getInt();
            temporal[i][0] = id;
            temporal[i][1] = heartBeat;
            temporal[i][2] = localTime;
        }

        long senderID = msgBytes.getLong();

         Arrays.sort(temporal, new Comparator<int[]>() {
            @Override
            public int compare(int[] first, int[] second) {
                if(first[0] > second[0]) return 1;
                else return -1;
            }
        });

        // add message to the list.
        long current = System.currentTimeMillis() - startTime;
        int currentTime = (int) current/100;
        String s =  this.myPeerServer.getServerId() + " Received Message from: " + senderID + " at current time: " + currentTime + logTable(temporal);
        receivedMessages.add(s);
        messageListLogger.info(s);


        // update our table with new received.
        updateTable(temporal,senderID);
        return temporal;
    }


    protected void logTable(){
       // logTable(this.failureDetection);
    }

    protected String logTable(int[][] temporal){
        StringBuilder s = new StringBuilder("\n TABLE: \n");
                for (int i = 0; i<temporal.length;i++){
                    s.append(temporal[i][0]).append("   ").append(+temporal[i][1]).append("  ").append(temporal[i][2]).append("\n");

                }
        return s.toString();
      //  logger.info("\nTABLE:\n"+ temporal[0][0] + "  " + +temporal[0][1] + " " +temporal[0][2] + "\n" +temporal[1][0] + "  " + +temporal[1][1] + " " +temporal[1][2] + "\n" +temporal[2][0] + "  " + +temporal[2][1] + " " +temporal[2][2] + "\n"+temporal[3][0] + "  " + +temporal[3][1] + " " +temporal[3][2] + "\n");
    }

    protected void logSmallTable(int[][] temporal){
        logger.info("\nTABLE:\n"+ temporal[0][0] + "  " + +temporal[0][1] + " " +temporal[0][2] + "\n" +temporal[1][0] + "  " + +temporal[1][1] + " " +temporal[1][2] + "\n" +temporal[2][0] + "  " + +temporal[2][1] + " " +temporal[2][2] +"\n");
    }




    protected byte[] integersToBytes(int[][] values, long senderID) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {

        for(int i=0; i < values.length; ++i){

                dos.writeInt(values[i][0]);
                dos.writeInt(values[i][1]);
                dos.writeInt(values[i][2]);
            }
            dos.writeLong(senderID);

           } catch (IOException e) {
                logger.info("catch! \n" + Util.getStackTrace(e));
            }

            return baos.toByteArray();
    }

    protected void updateMyHeartbeat(){


        long current = System.currentTimeMillis() - startTime;
        int currentTime = (int)current/100;

        for (int i = 0; i < failureDetection.length;i++){
            if (failureDetection[i][0] == myPeerServer.getServerId()){
                failureDetection[i][1] = this.heartBeatCounter;
                failureDetection[i][2] = currentTime;
            }
        }
    }


    protected void initFailureDetection(){

        failureDetection = new int[peerIDtoAddress.size()+1][3];

//     we are constructing the heartbeat table.
//        failureDetection[i][0]   -> server ID
//        failureDetection[i][1]   -> HeartbeatCount
//        failureDetection[i][2]   -> local time


      //  logger.info("constructing the table:");
        for (int i =0;i<failureDetection.length-1;i++) {
            failureDetection[i][0] = randomServerID[i];
            failureDetection[i][1] = -1;
            failureDetection[i][2] = (int) this.elapsedSeconds;
          //  logger.info(failureDetection[i][0] + " " + failureDetection[i][1] + "  " + failureDetection[i][2]);
        }
        // add myself to the table
        failureDetection[failureDetection.length-1][0] =  myPeerServer.getServerId().intValue();
        failureDetection[failureDetection.length-1][1] = heartBeatCounter;
        failureDetection[failureDetection.length-1][2] = (int) this.elapsedSeconds;
      //  logger.info(failureDetection[failureDetection.length-1][0] + " " + failureDetection[failureDetection.length-1][1] + "  " + failureDetection[failureDetection.length-1][2]);

        Arrays.sort(failureDetection, new Comparator<int[]>() {
            @Override
            public int compare(int[] first, int[] second) {
                if(first[0] > second[0]) return 1;
                else return -1;
            }
        });
    }


    protected void generateRandomArray(){
        Set<Long> r= new HashSet<>(peerIDtoAddress.keySet());
            Iterator<Long> iterator = r.iterator();
        for(int i =0; i< peerIDtoAddress.size();i++){
            long l = iterator.next();
            randomServerID[i] = (int) l;
        }
    }

    private Logger initializeHeartbeatLog(String fileNamePreface ) {

        boolean disableParentHandlers = true;
        Logger logger = Logger.getLogger(fileNamePreface);
        logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
          //  fileNamePreface.replace("edu.yu.cs.com3800", "");
            fh = new FileHandler("./log/" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
//        fh.setLevel(Level.INFO);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(disableParentHandlers);
        //fh.close();


        return logger;

    }
}


class LogHandle implements HttpHandler {

    Logger logger;
    ArrayList<String> messagesList = GossipHeartbeat.receivedMessages;


    @Override
    public void handle(HttpExchange t) throws IOException {

        this.logger = initializeLogging("Gossip-Handler");
        logger.info("Initializing Gossip Handler....");

        String method = t.getRequestMethod();
        Object header = t.getRequestHeaders().getFirst("Content-Type");


        InputStream is = t.getRequestBody();
        byte[] byteArray = toByteArray(is);
        InputStream loggingIs = new ByteArrayInputStream(byteArray);


        String inp;
        try (Scanner scanner = new Scanner(loggingIs, StandardCharsets.UTF_8.name())) {
            inp = scanner.useDelimiter("\\A").next();
        }

        logger.info("here ");
        StringBuilder allMessages = new StringBuilder();
        for (String str : messagesList) {
            allMessages.append(str);
        }
        logger.info(allMessages.toString());
        byte[] bytes = allMessages.toString().getBytes();


        t.sendResponseHeaders(200, bytes.length);
        OutputStream os = t.getResponseBody();
        os.write(bytes);
        os.close();
    }

    protected Logger initializeLogging(String fileNamePreface ) {

//        if (logger != null)return this.logger;

        boolean disableParentHandlers = true;
        Logger logger = Logger.getLogger(fileNamePreface);
        //logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
            fileNamePreface.replace("edu.yu.cs.com3800", "");
            fh = new FileHandler("./Log/LogFile_" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // fh.setLevel(Level.INFO);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.setUseParentHandlers(disableParentHandlers);
        //fh.close();

        return logger;

    }



    protected byte[] toByteArray(InputStream is) throws IOException {

        int num;
        byte[] data = new byte[16384];
        ByteArrayOutputStream myBuffer = new ByteArrayOutputStream();

        while ((num = is.read(data, 0, data.length)) != -1) {
            myBuffer.write(data, 0, num);
        }
        return myBuffer.toByteArray();
    }

}
