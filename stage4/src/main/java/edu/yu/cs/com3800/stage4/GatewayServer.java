package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GatewayServer  extends Thread implements LoggingServer {


    public static int myPort;
    long peerEpoch;
    long id;
    HttpServer server;
    Logger logger;
    Map<Long, InetSocketAddress> peerIDtoAddress;
    int observersNum;
    public long leaderID;
    public static int leaderUDPPort;
    GatewayPeerServerImpl gatewayPeer;

    /**
     creates an http server with this port.
     keeps track of what node is currently the leader and sends it all client requests over a TCP connection
     */
    public GatewayServer(int myPort, long peerEpoch, long id, Map<Long, InetSocketAddress> peerIDtoAddress, int observersNum){


        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
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
              leader = gatewayPeer.getCurrentLeader();
              if (leader!= null)  this.leaderID = leader.getProposedLeaderID();
        }



        logger.info( " leader id   " +   String.valueOf(leaderID));;
        this.leaderUDPPort = peerIDtoAddress.get(leaderID).getPort();


        // Step 2: create Http Server. Receive Requests from Clients
            this.server = HttpServer.create(new InetSocketAddress(myPort), 10);
            server.createContext("/compileandrun", new MyHandler());
            server.setExecutor(null);
            logger.info("starting HttpServer on port " + myPort);
            this.server.start();


        } catch (IOException e) {
            logger.info("error creating http Server");
            e.printStackTrace();
       }
    }

    public  GatewayPeerServerImpl getPeerServer(){
        return this.gatewayPeer;
    }


    public void shutDown() {
        this.server.stop(2);
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


    public void handle(HttpExchange t) throws IOException {


       this.logger = initializeLogging("GatewayHandler");
       logger.info("Initializing Gateway Handler....");

        String contentType = "text/x-java-source";
        String method = t.getRequestMethod();
        Object header = t.getRequestHeaders().getFirst("Content-Type");


        InputStream is = t.getRequestBody();
        byte[] byteArray = toByteArray(is);
        InputStream loggingIs = new ByteArrayInputStream(byteArray);


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
            // Step 1:  send Message to master
            logger.info("Starting  TCP on port - " + leaderTCPPort);
            Socket socket = new Socket("localhost", leaderTCPPort);

            OutputStream out = socket.getOutputStream();
            Message msg = new Message(Message.MessageType.WORK,byteArray, "localhost", GatewayServer.myPort,"localhost", leaderTCPPort);

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
            logger.info( "Gateway received response from Master \n" + workerResponse.toString());

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


    // logger for HTTP handler
    protected Logger initializeLogging(String fileNamePreface ) {

//        if (logger != null)return this.logger;

        boolean disableParentHandlers = true;
        Logger logger = Logger.getLogger(fileNamePreface);
        logger.setLevel(Level.FINER);


        FileHandler fh = null;
        try {
            fileNamePreface.replace("edu.yu.cs.com3800", "");
            fh = new FileHandler("./Log/LogFile_" + fileNamePreface  +".log");
        } catch (IOException e) {
            e.printStackTrace();
        }
        fh.setLevel(Level.INFO);
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
