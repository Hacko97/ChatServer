package chat.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import chat.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;

public class MessageReader implements Runnable {

	private BufferedReader reader; 
	private BlockingQueue<Message> messageQueue;
	
	public MessageReader(BufferedReader reader, BlockingQueue<Message> messageQueue) {
		this.reader = reader;
		this.messageQueue = messageQueue;
	}
	
	@Override
	public void run() {
		try {
			String clientMsg;
			while ((clientMsg = reader.readLine()) != null) {

                logger.trace("Message from client received: " + clientMsg);

                Message msg = new Message(true, clientMsg);
				messageQueue.add(msg);
			}

			Message exit = new Message(false, "exit");
			messageQueue.add(exit);
			
		} catch (SocketException e) {
			Message exit = new Message(false, "exit");
			messageQueue.add(exit);		
		} catch (IOException ioe) {
            if (ioe.getMessage().equalsIgnoreCase("Remote host closed connection during handshake")) {
                logger.warn("[KNOW ISSUE #1] Remote host closed connection during handshake");

            } else {

				logger.warn(ioe.getLocalizedMessage());
            }
		}
	}

	private static final Logger logger = LogManager.getLogger(MessageReader.class);
}
