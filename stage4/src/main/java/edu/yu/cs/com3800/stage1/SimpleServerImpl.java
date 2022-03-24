package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class SimpleServerImpl implements SimpleServer {

    int port;
    HttpServer server;



    String timeStamp = new SimpleDateFormat("MMdd_HHmmss").format(Calendar.getInstance().getTime());
    Logger logger = Logger.getLogger("LogFile");
    FileHandler fh;



    public SimpleServerImpl(int port) throws IOException{
        if (port < 1) throw new IllegalArgumentException("port cant be less than 1");


        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 10);

        server.createContext("/compileandrun", new MyHandler());
        server.setExecutor(null);
        startLog();
        logger.info("Constructing SimpleServerImpl...");


    }

    protected void startLog(){
        try {


            fh = new FileHandler("./LogFile.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.setUseParentHandlers(true);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void start() {
        this.server.start();
        logger.info(" Server started on port:  " + port);
    }

    public void stop() {
        server.stop(2);
        logger.info("Server has stopped");
    }

    static void main(String[] args) {
        int port = 9000;

        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        SimpleServer myserver = null;
        try {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

}


class MyHandler implements HttpHandler {

    Logger logger = Logger.getLogger("LogFile");

    public void handle(HttpExchange t) throws IOException {


        String response;
        String contentType = "text/x-java-source";
        String method = t.getRequestMethod();
        InputStream is = t.getRequestBody();
        Object header =  t.getRequestHeaders().getFirst("Content-Type");

        byte[] byteArray = toByteArray(is);
        InputStream newIs = new ByteArrayInputStream(byteArray);
        InputStream loggingIs = new ByteArrayInputStream(byteArray);


        logger.info(" Request Method is: " + method);

        // input stream to String
        String inp = null;
        try (Scanner scanner = new Scanner(loggingIs, StandardCharsets.UTF_8.name())) {
            inp = scanner.useDelimiter("\\A").next();
        }

        logger.info("INPUT: " + inp );




        if (!header.equals(contentType)){
            logger.info("ERROR - Content-Type don't match: " + contentType);
            byte[] resp = "".getBytes();
            t.sendResponseHeaders(400, resp.length);
            OutputStream os = t.getResponseBody();
            os.write(resp);
            os.close();
            is.readAllBytes();
            newIs.readAllBytes();
            // return; // REVISAR ESTE RETURN !!
        }


        JavaRunner runner = new JavaRunner();
        logger.info(" Starting JavaRunner... ");




        try {

            response =  runner.compileAndRun(newIs);
            logger.info("Response Code: " +  200);
            logger.info("OUTPUT:   "  + response);
            t.sendResponseHeaders(200,response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();


        } catch (Exception e) {

            is.readAllBytes();
            newIs.readAllBytes();

            logger.info(" OUTPUT:  " + e.getMessage());
            logger.info( "MESSAGE IS " + e.getMessage());

            logger.info(" Response code:  "  + 400);


            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string

            // String str = e.getMessage() + "\n" + e.printStackTrace();;
            t.sendResponseHeaders(400,e.getMessage().length());

            OutputStream os = t.getResponseBody();
            os.write(e.getMessage().getBytes());
            os.close();


            //   String s  = e.printStackTrace();
            e.printStackTrace();
        }
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




