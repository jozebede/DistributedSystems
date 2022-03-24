package edu.yu.cs.com3800;

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDPMessageSender extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> outgoingMessages;
    private Logger logger;
    private int serverUdpPort;

    public UDPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, int serverUdpPort) {
        this.outgoingMessages = outgoingMessages;
        setDaemon(true);
        this.serverUdpPort = serverUdpPort;
        setName("UDPMessageSender-port-" + this.serverUdpPort);

    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {

        while (!this.isInterrupted()) {

            try {
                if(this.logger == null){
                    this.logger = initializeLogging(UDPMessageSender.class.getName() + "-on-server-with-udpPort-" + this.serverUdpPort);
                }
                Message messageToSend = this.outgoingMessages.poll(2, TimeUnit.SECONDS);
                if (messageToSend != null) {
                    DatagramSocket socket = new DatagramSocket();
                    byte[] payload = messageToSend.getNetworkPayload();
                    DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, new InetSocketAddress(messageToSend.getReceiverHost(), messageToSend.getReceiverPort()));
                    socket.send(sendPacket);
                    socket.close();
                   logger.info("Message sent:\n" + messageToSend.toString());
                  this.logger.fine("Message sent:\n" + messageToSend.toString());
                }
            }
            catch(InterruptedException e){}
            catch (Exception e) {
                logger.info("WARNING, failed to send packet");
                this.logger.log(Level.WARNING,"failed to send packet", e);
            }
        }
    }//run

}