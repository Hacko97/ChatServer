package chat.model;

import chat.service.ClientConnection;

import javax.net.ssl.SSLSocket;
import java.net.Socket;

public class UserInfo {
    private String identity;
    private String currentChatRoom;
    private Socket socket;
    private ClientConnection managingThread;
    private boolean roomOwner = false;

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getCurrentChatRoom() {
        return currentChatRoom;
    }

    public void setCurrentChatRoom(String currentChatRoom) {
        this.currentChatRoom = currentChatRoom;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public ClientConnection getManagingThread() {
        return managingThread;
    }

    public void setManagingThread(ClientConnection managingThread) {
        this.managingThread = managingThread;
    }

    public boolean isRoomOwner() {
        return roomOwner;
    }

    public void setRoomOwner(boolean roomOwner) {
        this.roomOwner = roomOwner;
    }
}
