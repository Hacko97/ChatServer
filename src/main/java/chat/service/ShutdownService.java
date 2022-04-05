package chat.service;

import chat.server.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import chat.server.ServerInfo;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ShutdownService extends Thread {

    private ServerState serverState = ServerState.getInstance();
    private ServerInfo serverInfo = serverState.getServerInfo();
    private ExecutorService pool;

    public ShutdownService(ExecutorService pool) {
        this.pool = pool;
    }

    @Override
    public void run() {
        logger.info("Server is shutting down. Please wait...");

        serverState.stopRunning(true);

        pool.shutdown();

        removeMyChatRoomsOnPeers();

        try {
            if (!pool.awaitTermination(SHUTDOWN_TIME, TimeUnit.MILLISECONDS)) {
                logger.warn("Executor pool did not shutdown in the specified time.");
                List<Runnable> droppedTasks = pool.shutdownNow();
                logger.warn("Executor pool was abruptly shutdown. " + droppedTasks.size() + " tasks will not be executed.");
            }
        } catch (InterruptedException e) {
            logger.trace(e.getMessage());

        }
    }

    private void removeMyChatRoomsOnPeers() {
        JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();
        ServerCommunication peerClient = new ServerCommunication();

        for (ServerInfo server : serverState.getServerInfoList()) {
            if (server.equals(serverInfo)) continue;

            for (String roomId : serverState.getLocalChatRooms().keySet()) {
                peerClient.commPeer(server, messageBuilder.deleteRoomPeers(roomId));
            }
        }
    }

    private static final long SHUTDOWN_TIME = TimeUnit.SECONDS.toMillis(10);
    private static final Logger logger = LogManager.getLogger(ShutdownService.class);
}
