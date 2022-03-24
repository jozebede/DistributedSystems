package edu.yu.cs.com3800.stage1;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;


public class ClientImpl implements Client {

    String hostName;
    int hostPort;
    Response response;
    HttpURLConnection httpClient;
    URL url;


    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        if (hostName == null || hostPort < 0) throw new IllegalArgumentException();
        // port valid range?

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


        httpClient.setRequestProperty("Content-Type", "text/x-java-source");
        httpClient.setRequestMethod("POST");
        httpClient.setDoOutput(true);
        httpClient.setChunkedStreamingMode(src.length());

        // send data by writing to the output stream of the connection
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

            // httpClient.disconnect();

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

