package edu.yu.cs.com3800.stage4;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


import edu.yu.cs.com3800.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Stage4Test {

    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private String sumClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class Sum\n{\n    public String run()\n    {\n     int t = 2+2; \n   return String.valueOf(t);\n    }\n}\n";
    private String classWithError = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class Sum\n{\n    public int run()\n    {\n     int t = 2+2; \n   return String.valueOf(t);\n    }\n}\n";

    //private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070};  // in this case, leader is 6
    private int[] ports = {8010, 8020, 8030}; // leader  and 2 workers
    private String gatewayHost = "localhost";
    private int gatewayPort = 8888;
    GatewayServer gatewayServer;
    private int gateID = 8;

    private ArrayList<ZooKeeperPeerServer> servers;
    protected int leaderId = 2; //6; //ports[ports.length-1];
    HttpURLConnection httpClient0;
    HttpURLConnection httpClient1;
    HttpURLConnection httpClient2;
    HttpURLConnection httpClient3;
    URL url;




    /* note: Shutdown bug! JavaRunnerFollower and RoundRobinLeader  throws exception
    Interrupted function call: accept failed
    java.net.SocketException: Interrupted function call: accept failed */


    @Test
    public void leaderCheck(){

        createServers();

        for (ZooKeeperPeerServer server : this.servers) {
            assertTrue(server.getCurrentLeader().getProposedLeaderID() == leaderId, "\n" + "Expected response:" +"\n" + "Leader ID: " + leaderId +"\n" + "Actual response:" + "\n" + "Leader ID: " +server.getCurrentLeader().getProposedLeaderID() + "\n");
        }

        stopServers();
    }


    @Test
    public void javaClassWithErrors() throws Exception {

      createServers();

      this.httpClient0 = sendMessage(classWithError);
      Client.Response HttpResponse0 =  getHTTPMsg(this.httpClient0);
      assertEquals(400 ,HttpResponse0.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse0.getCode());

      String errorString = "Code did not compile:Error on line 8, column 25 in string:///edu/yu/cs/fall2019/com3800/stage1/Sum.java";
      assertEquals(errorString,HttpResponse0.getBody().trim(),  "\nExpected Response:\n" + errorString +"\n" + "Actual response:" + "\n" +HttpResponse0.getBody());

      stopServers();
    }

    @Test
    public void SingleRequest() throws Exception {

        createServers();

        this.httpClient0 = sendMessage(sumClass);

        Client.Response HttpResponse0 =  getHTTPMsg(this.httpClient0);
        assertEquals(200 ,HttpResponse0.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse0.getCode());
        assertEquals("4",HttpResponse0.getBody(),"Expected response:" +"\n" + "4" +"\n" + "Actual response:" + "\n" +HttpResponse0.getBody());

        stopServers();
    }



    @Test
    public void MultipleRequests() throws Exception {

        //step 2: create servers
        createServers();



        //step 2: send request to the Gateway
        String code0 = this.validClass.replace("world!", "world! from code version " + 0);
        String code1 = this.validClass.replace("world!", "world! from code version " + 1);
        String code2 = this.validClass.replace("world!", "world! from code version " + 2);
        String code3 = this.validClass.replace("world!", "world! from code version " + 3);

        this.httpClient0 = sendMessage(code0);
        this.httpClient1 = sendMessage(code1);
        this.httpClient2 = sendMessage(code2);
        this.httpClient3 = sendMessage(code3);

        //step 3: validate responses from Gateway
        Client.Response HttpResponse0 =  getHTTPMsg(this.httpClient0);
        assertEquals(200 ,HttpResponse0.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse0.getCode());
        assertEquals("Hello world! from code version 0",HttpResponse0.getBody(),"Expected response:" +"\n" + "hello world" +"\n" + "Actual response:" + "\n" +HttpResponse0.getBody());

        Client.Response HttpResponse1 =  getHTTPMsg(this.httpClient1);
        assertEquals(200 ,HttpResponse1.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse1.getCode());
        assertEquals("Hello world! from code version 1",HttpResponse1.getBody(),"Expected response:" +"\n" + "hello world" +"\n" + "Actual response:" + "\n" +HttpResponse1.getBody());

        Client.Response HttpResponse2 =  getHTTPMsg(this.httpClient2);
        assertEquals(200 ,HttpResponse2.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse2.getCode());
        assertEquals("Hello world! from code version 2",HttpResponse2.getBody(),"Expected response:" +"\n" + "hello world" +"\n" + "Actual response:" + "\n" +HttpResponse2.getBody());

        Client.Response HttpResponse3 =  getHTTPMsg(this.httpClient3);
        assertEquals(200 ,HttpResponse3.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +HttpResponse3.getCode());
        assertEquals("Hello world! from code version 3",HttpResponse3.getBody(),"Expected response:" +"\n" + "hello world" +"\n" + "Actual response:" + "\n" +HttpResponse3.getBody());


        //step 4: stop servers and clients
        stopServers();
    }


    private void stopServers() {

        for (ZooKeeperPeerServer server : this.servers) {
            server.shutdown();
        }
        this.gatewayServer.shutDown();

        if (httpClient0!= null)httpClient0.disconnect();
        if (httpClient1!= null) httpClient1.disconnect();
        if (httpClient2!= null) httpClient2.disconnect();
        if (httpClient3!= null) httpClient3.disconnect();

       // wait for every Thread Shut down
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




    /**
     * Receives Http response from Gateway.
     */
    private Client.Response getHTTPMsg(HttpURLConnection client) throws IOException {
        Client.Response response;
        InputStreamReader inReader;

        if (client.getResponseCode() == 400){
            inReader = new InputStreamReader(client.getErrorStream());
    }else{
            inReader = new InputStreamReader(client.getInputStream());
        }
        try (BufferedReader in = new BufferedReader(inReader)) {

            StringBuilder responseBody = new StringBuilder();
            String line;

            while ((line = in.readLine()) != null) {
                responseBody.append(line);
            }

            in.close();
            response = new Client.Response(client.getResponseCode(), responseBody.toString());


        } catch (IOException e) {
            InputStream errorStream =   client.getErrorStream();
            String text = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
            response = new Client.Response(client.getResponseCode(), text);
            e.printStackTrace();
        }
        return response;
    }


    /**
     * sends Http request to Gateway
     * @param code Java Code
     */
    private HttpURLConnection sendMessage(String code) throws Exception {

            url = new URL("http", gatewayHost, gatewayPort, "/compileandrun");
        HttpURLConnection client = (HttpURLConnection) url.openConnection();
            client.setRequestProperty("Content-Type", "text/x-java-source");
            client.setRequestMethod("POST");
            client.setDoOutput(true);
            client.setChunkedStreamingMode(code.length());
            OutputStream  outputStream = null;

        try {
            outputStream = client.getOutputStream();
            OutputStreamWriter osWriter = new OutputStreamWriter(outputStream, "UTF-8");
            osWriter.write(code);
            osWriter.flush();
            osWriter.close();
            outputStream.close();
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }


    private void createServers() {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(4);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }

        HashMap<Long, InetSocketAddress> m = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();


        //create servers
        this.servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey()); // removes itself
            map.put(Integer.valueOf(gateID).longValue(), new InetSocketAddress("localhost", this.gatewayPort));
            ZooKeeperPeerServer server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 1);
            this.servers.add(server);
            new Thread((Runnable) server, "Server on port " + server.getAddress().getPort()).start();


           }
        try {
        Thread.sleep(4000);
    } catch (Exception e) {
    }


     this.gatewayServer = new GatewayServer(gatewayPort, 0, Integer.valueOf(gateID).longValue(), m, 1);
        GatewayPeerServerImpl gatewayPeer =  this.gatewayServer.getPeerServer();
        new Thread((Runnable)  this.gatewayServer, "Gateway on port " + gatewayPeer.getAddress().getPort()).start();
        servers.add(gatewayPeer);

        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }
    }
}


