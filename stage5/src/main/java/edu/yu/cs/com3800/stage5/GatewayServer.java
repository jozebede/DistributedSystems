package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.LinkOption;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GatewayServer  extends Thread implements LoggingServer {


    public static int myPort;
    long peerEpoch;
    static AtomicLong requestCount = new AtomicLong(0);
    long id;
    HttpServer server;
    Logger logger;
    static Map<Long, InetSocketAddress> peerIDtoAddress;
    int observersNum;
    public static long leaderID;
    public  static int leaderUDPPort;
    public  static GatewayPeerServerImpl gatewayPeer;
    private LinkedList<InetSocketAddress> failedServersAddress;

    /**
     creates an http server with this port.
     keeps track of what node is currently the leader and sends it all client requests over a TCP connection
     */
    public GatewayServer(int myPort, long peerEpoch, long id, Map<Long, InetSocketAddress> peerIDtoAddress, int observersNum){


        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.failedServersAddress = new LinkedList<>();
        this.observersNum = observersNum;
        this.logger = initializeLogging(this.getClass().getName() + "-on-port-" + myPort);
        logger.info(" init GatewayServer on port-" + this.myPort);
        setName("GatewayServer-port-" + this.myPort);

        // create GatewayPeerServerImpl Thread
        gatewayPeer = new GatewayPeerServerImpl(myPort,peerEpoch,id,peerIDtoAddress,observersNum);
        gatewayPeer.start();

        try {

         //Step 1: Find the Leader
            Vote leader = gatewayPeer.getCurrentLeader();
            if (leader!= null){
                this.leaderID = leader.getProposedLeaderID();
            } else {    // check again
                Thread.sleep(2000); // maybe the election has not finalized yet
              leader = gatewayPeer.getCurrentLeader();
              if (leader!= null)  this.leaderID = leader.getProposedLeaderID();
            }


        this.leaderUDPPort = peerIDtoAddress.get(leaderID).getPort();
        logger.info( " leader id   " +   String.valueOf(this.leaderID) + "\nLeader port: " +this.leaderUDPPort );





            // Step 2: create Http Server. Receive Requests from Clients
            logger.info("starting HttpServer on port " + myPort);
            if(this.server == null) {
                this.server = HttpServer.create(new InetSocketAddress(myPort), 8);
                server.createContext("/compileandrun", new MyHandler());
                server.setExecutor(null);
                this.server.start();
            }


        } catch (IOException e) {
            logger.info("error creating http Server \n" + Util.getStackTrace(e));
            e.printStackTrace();
       } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




    public  GatewayPeerServerImpl getPeerServer(){
        return this.gatewayPeer;
    }




    public void shutDown() {
        this.server.stop(0);
    }


    @Override
    public void start() {
        this.server.start();
        logger.info(" Server started on port:  " + myPort);
    }




    } // GatewayPeerServerImpl


class MyHandler implements HttpHandler {


    Logger logger;
    int leaderTCPPort = GatewayServer.leaderUDPPort +2;
    long leaderID = GatewayServer.leaderID;
    GatewayPeerServerImpl gatewayPeer = GatewayServer.gatewayPeer;
    Socket socket;
    static Map<Long, InetSocketAddress> peerIDtoAddress = GatewayServer.peerIDtoAddress;



    public void handle(HttpExchange t) throws IOException {


       this.logger = initializeLogging("GatewayHandler");
       logger.info("Initializing Gateway Handler....");

        String contentType = "text/x-java-source";
        String method = t.getRequestMethod();
        Object header = t.getRequestHeaders().getFirst("Content-Type");


        InputStream is = t.getRequestBody();
        byte[] byteArray = toByteArray(is);
        InputStream loggingIs = new ByteArrayInputStream(byteArray);

        if (GatewayServer.gatewayPeer.isPeerDead(leaderID)){
            logger.info("leader is dead!");
           int newLeaderUDP =  updateLeader();
           leaderTCPPort = newLeaderUDP+2;
        }else {
            //logger.info("leader is OK!");
        }

        String inp;
        try (Scanner scanner = new Scanner(loggingIs, StandardCharsets.UTF_8.name())) {
            inp = scanner.useDelimiter("\\A").next();
        }
        logger.info("received HTTP message from client: " +"\n" +"Request method: " + method + " \nHeader:  " + header.toString() + "\nMessage: \n" + inp);



        if (!header.equals(contentType)) {
            logger.info("ERROR - Content-Type don't match: " + contentType);
            byte[] resp = "".getBytes();
            //send empty response to the client
            t.sendResponseHeaders(400, resp.length);
            OutputStream os = t.getResponseBody();
            os.write(resp);
            os.close();
            is.readAllBytes();
           // data.readAllBytes();
            return; // check this return
        }


        try {
            // if leader is dead, gateway locally queue the request until new leader is elected
            if(gatewayPeer.isPeerDead(leaderID)){
                updateLeader();
            }

            // Step 1:  send Message to master
            logger.info("Starting  TCP on port - " + leaderTCPPort);
            socket = new Socket("localhost", leaderTCPPort);

            OutputStream out = socket.getOutputStream();
            long requestID = GatewayServer.requestCount.addAndGet(1);
            Message msg = new Message(Message.MessageType.WORK,byteArray, "localhost", GatewayServer.myPort,"localhost", leaderTCPPort, requestID);

            byte[] data = msg.getNetworkPayload();
            out.write( data);
            logger.info("Message sent to Master");
            //out.close();


            // Step 2: get response from master
            logger.info("getting response from master");
            String answer = "";
            InputStream in = socket.getInputStream();

            byte[] receiveBuf = new byte[400];
            in.read(receiveBuf);

            Message workerResponse = new Message(receiveBuf);


            //if the Gateway had already sent some work to the leader before the leader was marked as
            //failed and the (now failed) leader sends a response to the gateway after it was marked failed,
            // the gateway will NOT accept that response;
            // it will ignore that response from the leader and queue up the request to send to the new leader for a response.

            if (GatewayServer.gatewayPeer.isPeerDead(leaderID)){
                logger.info(" received message from a Dead leader!");
                int newLeaderUDP =  updateLeader();
                leaderTCPPort = newLeaderUDP+2;
                updateLeader();
                //send the request to the new leader:
                leaderTCPPort = peerIDtoAddress.get(leaderID).getPort();

                try {
                    this.socket.close();
                    this.socket = null;
                    logger.info("Starting  TCP on port - " + leaderTCPPort);
                    this.socket = new Socket("localhost", leaderTCPPort);

                    out = socket.getOutputStream();
                    requestID = GatewayServer.requestCount.addAndGet(1);
                    msg = new Message(Message.MessageType.WORK,null, "localhost", GatewayServer.myPort,"localhost", leaderTCPPort, requestID);

                    data = msg.getNetworkPayload();
                    out.write( data);
                    logger.info("Message sent to new Master on port " + leaderTCPPort);
                    //out.close();






                } catch (IOException e) {
                    e.printStackTrace();
                }


            }else {
                logger.info("Gateway received response from Master \n" + workerResponse.toString());
            }

            socket.close();


            // Step 3: Send response to the client
            int responseCode = workerResponse.getErrorOccurred()? 400:200;
            t.sendResponseHeaders(responseCode, workerResponse.getMessageContents().length);
            OutputStream os = t.getResponseBody();
            os.write(workerResponse.getMessageContents());
            os.close();
            logger.info("response sent back to client");


        } catch (Exception e) {

            is.readAllBytes();
             logger.info("ERROR  \n  " + Util.getStackTrace(e) +  "\n" + e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
    }

    protected int updateLeader() {
        logger.info("UPDATING LEADER");
        Vote leader = gatewayPeer.getCurrentLeader();
        if (leader != null) {
            leaderID = leader.getProposedLeaderID();
        } else {    // check again
            leader = gatewayPeer.getCurrentLeader();
            if (leader != null) leaderID = leader.getProposedLeaderID();
        }
        logger.info("found leader   " + leader );


       return  peerIDtoAddress.get(leaderID).getPort();
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
