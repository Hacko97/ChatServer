package chat.service;

import chat.server.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import chat.model.Protocol;
import chat.improvements.ManageClientAndServerMessage;
import chat.model.Message;
import chat.model.UserInfo;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ClientConnection implements Runnable {

    private BufferedReader reader;
    private BufferedWriter writer;
    private ExecutorService pool;
    private JSONParser parser;

    private Socket clientSocket;
    private BlockingQueue<Message> messageQueue;
    private UserInfo userInfo;
    private boolean routed = false;


    private ServerState serverState = ServerState.getInstance();

    public ClientConnection(Socket clientSocket) {
        try {
            this.clientSocket = clientSocket;
            this.reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));
            this.writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
            this.messageQueue = new LinkedBlockingQueue<>();
            this.parser = new JSONParser();
            this.pool = Executors.newSingleThreadExecutor();
        } catch (Exception e) {
            logger.trace(e.getMessage());
        }
    }

    @Override
    public void run() {

        try {

            pool.execute(new MessageReader(reader, messageQueue));

            while (true) {

                Message msg = messageQueue.take();
                logger.trace("Processing client messages: " + msg.getMessage());

                if (!msg.isFromClient() && msg.getMessage().equalsIgnoreCase("exit")) {
                    ManageClientAndServerMessage.newClientMessageHandler(null, this);
                    logger.trace("EOF");
                    break;
                }

                if (msg.isFromClient()) {

                    JSONObject jsonMessage = (JSONObject) parser.parse(msg.getMessage());

                    ManageClientAndServerMessage.newClientMessageHandler(jsonMessage, this);

                    String type = (String) jsonMessage.get(Protocol.type.toString());
                    if (type.equalsIgnoreCase(Protocol.quit.toString())) break;

                } else {
                    write(msg.getMessage());
                }
            }

            pool.shutdown();
            writer.close();
            reader.close();
            clientSocket.close();

            if (userInfo != null) {
                logger.info("Client disconnected: " + userInfo.getIdentity());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            pool.shutdownNow();
        } finally {

            if (!clientSocket.isClosed()) {
                try {
                    writer.close();
                    reader.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void write(String msg) {
        try {
            writer.write(msg + "\n");
            writer.flush();

            logger.trace("Message flush");

        } catch (IOException e) {
            logger.trace(e.getMessage());
        }
    }

    public BlockingQueue<Message> getMessageQueue() {
        return messageQueue;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    public UserInfo getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(UserInfo userInfo) {
        this.userInfo = userInfo;
    }

    public boolean isRouted() {
        return routed;
    }

    public void setRouted(boolean routed) {
        this.routed = routed;
    }

    private static final Logger logger = LogManager.getLogger(ClientConnection.class);
}
