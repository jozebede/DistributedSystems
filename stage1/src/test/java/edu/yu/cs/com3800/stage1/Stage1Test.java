package edu.yu.cs.com3800.stage1;


import static org.junit.jupiter.api.Assertions.*;


import edu.yu.cs.com3800.SimpleServer;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;


public class Stage1Test{

    // LOG FILE LOCATION: ZebedeJoey\com3800\stage


    String helloworld =  "public class HelloWorld{ public HelloWorld(){ } public String run(){return \"hello world\";}} ";
    String testingClass = "public class testingClass{ public testingClass(){  } public String run(){ return \"return from testingClass!\"; }}";
    String classWithError = "public class testingClass{ public testingClass(){ } public int run(){ return \"this is the return method from TESTING CLASS !!\"; }}";
    String classWithError2 =  "public class HelloWorld{ public HelloWorld(){ } public String run(){return 2;}}";
    String classWithoutRun = "public class testingClass{ public testingClass(){  } public String runing(){ return \"return from testingClass!\"; }}";
    String classWithWrongReturnType = "public class testingClass{ public testingClass(){  } public int run(){ return \"return from testingClass!\"; }}";




    @Test
    void testingMultipleRequests() throws IOException {

        SimpleServer myServer = new SimpleServerImpl(1);
        myServer.start();

        ClientImpl client = new ClientImpl("localhost",1);

        // request 1
        client.sendCompileAndRunRequest(helloworld);
        Client.Response r = client.getResponse();
        assertEquals(200 ,r.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +r.getCode());
        assertEquals("hello world",r.getBody(),"Expected response:" +"\n" + "hello world" +"\n" + "Actual response:" + "\n" +r.getBody());

        // request 2
        client.sendCompileAndRunRequest(testingClass);
        Client.Response r1 = client.getResponse();
        assertEquals( 200,r1.getCode(),"\n" + "Expected response:" +"\n" + "200" +"\n" + "Actual response:" + "\n" +r1.getCode());
        assertEquals("return from testingClass!",r1.getBody(),"\n" + "Expected response:" +"\n" + "return from testingClass!" +"\n" + "Actual response:" + "\n" +r1.getBody());


        myServer.stop();
    }

    @Test
    void testingWrongJavaClasses() throws IOException {
        SimpleServer myServer = new SimpleServerImpl(8000);
        myServer.start();

        ClientImpl client = new ClientImpl("localhost",8000);


        client.sendCompileAndRunRequest(classWithError);
        Client.Response r2 = client.getResponse();
        String expectedBody = " Code did not compile:\n" +"Error on line 1, column 78 in string:///testingClass.java\n";
        assertEquals( 400,r2.getCode(),"\n" + "Expected response:" +"\n" + "400" +"\n" + "Actual response:" + "\n" +r2.getCode());
        assertEquals(expectedBody.length(), r2.getBody().length(),"\n" + "Expected response:" +"\n" + expectedBody +"\n" + "Actual response:" + "\n" +r2.getBody());

        client.sendCompileAndRunRequest(classWithError2);
        Client.Response r3 = client.getResponse();
        String expectedBody3 = "Code did not compile:\n" + "Error on line 1, column 76 in string:///HelloWorld.java\n";
        assertEquals( 400,r2.getCode(),"\n" + "Expected response:" +"\n" + "400" +"\n" + "Actual response:" + "\n" +r3.getCode());
        assertTrue(expectedBody3.replaceAll("\\s+","").equalsIgnoreCase(r3.getBody().replaceAll("\\s+","")),"\n" + "Expected response:" +"\n" + expectedBody3 +"\n" + "Actual response:" + "\n" +r3.getBody());


        client.sendCompileAndRunRequest(classWithoutRun);
        Client.Response r4 = client.getResponse();
        String withoutRun = "Could not create and run instance of class";
        assertEquals( 400,r4.getCode(),"\n" + "Expected response:" +"\n" + "400" +"\n" + "Actual response:" + "\n" +r4.getCode());
        assertTrue(withoutRun.replaceAll("\\s+","").equalsIgnoreCase(r4.getBody().replaceAll("\\s+","")),"\n" + "Expected response:" +"\n" + withoutRun +"\n" + "Actual response:" + "\n" +r4.getBody());

        client.sendCompileAndRunRequest(classWithWrongReturnType);
        Client.Response r5 = client.getResponse();
        String wrongreturn = "Could not create and run instance of class";
        assertEquals( 400,r5.getCode(),"\n" + "Expected response:" +"\n" + "400" +"\n" + "Actual response:" + "\n" +r5.getCode());
        assertTrue(withoutRun.replaceAll("\\s+","").equalsIgnoreCase(r4.getBody().replaceAll("\\s+","")),"\n" + "Expected response:" +"\n" + withoutRun +"\n" + "Actual response:" + "\n" +r4.getBody());

    }

        @Test
    void TestingStartAndStop() throws IOException{

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            SimpleServer NoArgsServer = new SimpleServerImpl(0);
        });

        Exception exception1 = assertThrows(IllegalArgumentException.class, () -> {
            ClientImpl noArgsClient = new ClientImpl(null,0);
        });
    }

    @Test
    void TestingContentType() throws IOException {

        SimpleServer serv = new SimpleServerImpl(200);
        serv.start();


        WrongClient c = new WrongClient("localhost",200);

        c.sendCompileAndRunRequest(helloworld);
        Client.Response r = c.getResponse();
        assertEquals(400, r.getCode(),"\n" + "Expected response:" +"\n" + "400" +"\n" + "Actual response:" + "\n" +r.getCode());
        assertEquals("", r.getBody(), "\n" + "Expected response:" +"\n" + "" +"\n" + "Actual response:" + "\n" +r.getBody());

        serv.stop();
    }



    }











 class WrongClient implements Client {

    String hostName;
    int hostPort;
    Response response;
    HttpURLConnection httpClient;
    URL url;


    public WrongClient (String hostName, int hostPort) throws MalformedURLException {

        this.hostName = hostName;
        this.hostPort = hostPort;
        url = new URL("http", hostName , hostPort, "/compileandrun");
    }

    public void sendCompileAndRunRequest(String src) throws IOException {
        if (src == null)throw new IllegalArgumentException("src can't be bull");

        try {
            this.httpClient  = (HttpURLConnection) url.openConnection();

        } catch (IOException e) {
            e.printStackTrace();
        }

        httpClient.setRequestProperty("Content-Type", "OTHER");
        httpClient.setRequestMethod("POST");
        httpClient.setDoOutput(true);
        httpClient.setChunkedStreamingMode(src.length());

        OutputStream outputStream = httpClient.getOutputStream();
        OutputStreamWriter osWriter = new OutputStreamWriter(outputStream, "UTF-8");
        osWriter.write(src);
        osWriter.flush();
        osWriter.close();
        outputStream.close();
        httpClient.connect();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()))) {
            StringBuilder responseBody = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                responseBody.append(line);
            }
            in.close();
            response = new Response(httpClient.getResponseCode(),responseBody.toString());
        } catch (IOException e) {
            InputStream trrr =   httpClient.getErrorStream();

            String text = new String(trrr.readAllBytes(), StandardCharsets.UTF_8);

            response = new Response(httpClient.getResponseCode(),text);
            // httpClient.disconnect();
            e.printStackTrace();
        }

        httpClient.disconnect();
    }
    public Response getResponse() throws IOException {
        return response;
    }
}




