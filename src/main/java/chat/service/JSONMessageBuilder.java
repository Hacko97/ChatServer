package chat.service;

import chat.server.ServerState;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import chat.model.Protocol;
import chat.server.ServerInfo;
import chat.model.ChatRoom;
import chat.model.LocalChatRoom;

import java.time.Instant;
import java.util.stream.Collectors;

public class JSONMessageBuilder {

    private static JSONMessageBuilder instance = null;

    private JSONMessageBuilder() {
    }

    public static synchronized JSONMessageBuilder getInstance() {
        if (instance == null) instance = new JSONMessageBuilder();
        return instance;
    }

    private final ServerState serverState = ServerState.getInstance();
    private final ServerInfo serverInfo = serverState.getServerInfo();

    public String serverChange(String approved, String serverId) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.serverchange.toString());
        jj.put(Protocol.approved.toString(), approved);
        jj.put(Protocol.serverid.toString(), serverId);
        return jj.toJSONString();
    }

    public String route(String joiningRoomId, String host, Integer port) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.route.toString());
        jj.put(Protocol.roomid.toString(), joiningRoomId);
        jj.put(Protocol.host.toString(), host);
        jj.put(Protocol.port.toString(), port.toString());
        return jj.toJSONString();
    }


    public String message(String identity, String content) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.message.toString());
        jj.put(Protocol.identity.toString(), identity);
        jj.put(Protocol.content.toString(), content);
        jj.put(Protocol.timestamp.toString(), Instant.now().toString());
        return jj.toJSONString();
    }

    public String deleteRoom(String roomId, String approved) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.deleteroom.toString());
        jj.put(Protocol.roomid.toString(), roomId);
        jj.put(Protocol.approved.toString(), approved);
        return jj.toJSONString();
    }

    public String deleteRoomPeers(String roomId) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.deleteroom.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.roomid.toString(), roomId);
        return jj.toJSONString();
    }

    public String releaseRoom(String roomId, String approved) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.releaseroomid.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.roomid.toString(), roomId);
        jj.put(Protocol.approved.toString(), approved);
        return jj.toJSONString();
    }

    public String lockRoom(String roomId) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.lockroomid.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.roomid.toString(), roomId);
        return jj.toJSONString();
    }

    public String createRoomResp(String roomId, String approved) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.createroom.toString());
        jj.put(Protocol.roomid.toString(), roomId);
        jj.put(Protocol.approved.toString(), approved);
        return jj.toJSONString();
    }

    public String whoByRoom(String room) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.roomcontents.toString());
        jj.put(Protocol.roomid.toString(), room);
        LocalChatRoom localChatRoomInfo = serverState.getLocalChatRooms().get(room);
        JSONArray ja = new JSONArray();
        ja.addAll(localChatRoomInfo.getMembers());
        jj.put(Protocol.identities.toString(), ja);
        jj.put(Protocol.owner.toString(), localChatRoomInfo.getOwner());
        return jj.toJSONString();
    }

    public String listRooms() {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.roomlist.toString());
        JSONArray ja = new JSONArray();
        for (LocalChatRoom localChatRoomInfo : serverState.getLocalChatRooms().values()) {
            String chatRoomId = localChatRoomInfo.getChatRoomId();
            ja.add(chatRoomId);
        }
        ja.addAll(serverState.getRemoteChatRooms().values().stream()
                .map(ChatRoom::getChatRoomId)
                .collect(Collectors.toList()));

        jj.put(Protocol.rooms.toString(), ja);

        return jj.toJSONString();
    }

    public String listServers() {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.serverlist.toString());

        JSONArray ja = new JSONArray();

        for (ServerInfo server : serverState.getServerInfoList()) {
            if (serverState.isOnline(server)) {
                JSONObject jo = new JSONObject();
                jo.put(Protocol.serverid.toString(), server.getServerId());
                jo.put(Protocol.address.toString(), server.getAddress());
                jo.put(Protocol.port.toString(), server.getPort());
                ja.add(jo);
            }
        }

        jj.put(Protocol.servers.toString(), ja);

        return jj.toJSONString();
    }

    public String releaseIdentity(String userId) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.releaseidentity.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.identity.toString(), userId);
        return jj.toJSONString();
    }

    public String lockIdentity(String userId) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.lockidentity.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.identity.toString(), userId);
        return jj.toJSONString();
    }

    public String newIdentityResp(String approve) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.newidentity.toString());
        jj.put(Protocol.approved.toString(), approve);
        return jj.toJSONString();
    }

    public String roomChange(String former, String roomId, String identity) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.roomchange.toString());
        jj.put(Protocol.identity.toString(), identity);
        jj.put(Protocol.former.toString(), former);
        jj.put(Protocol.roomid.toString(), roomId);
        return jj.toJSONString();
    }

    public String lockIdentity(String serverId, String userId, String locked) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.lockidentity.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.identity.toString(), userId);
        jj.put(Protocol.locked.toString(), locked);
        return jj.toJSONString();
    }

    public String lockRoom(String serverId, String roomId, String locked) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.lockroomid.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.roomid.toString(), roomId);
        jj.put(Protocol.locked.toString(), locked);
        return jj.toJSONString();
    }

    public String listRoomsClient() {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.list.toString());
        return jj.toJSONString();
    }


    public String notifyServerDownMessage(String serverId) {

        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.notifyserverdown.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        return jj.toJSONString();
    }

    public String serverUpMessage() {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.serverup.toString());
        jj.put(Protocol.serverid.toString(), serverInfo.getServerId());
        jj.put(Protocol.address.toString(), serverInfo.getAddress());
        jj.put(Protocol.port.toString(), serverInfo.getPort());
        jj.put(Protocol.managementport.toString(), serverInfo.getManagementPort());
        return jj.toJSONString();
    }

    public String startElectionMessage(String serverId, String serverAddress, Long serverPort, Long
            serverManagementPort) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.startelection.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.address.toString(), serverAddress);
        jj.put(Protocol.port.toString(), String.valueOf(serverPort));
        jj.put(Protocol.managementport.toString(), String.valueOf(serverManagementPort));
        return jj.toJSONString();
    }

    public String electionAnswerMessage(String serverId, String serverAddress, Integer serverPort, Integer
            serverManagementPort) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.answerelection.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.address.toString(), serverAddress);
        jj.put(Protocol.port.toString(), String.valueOf(serverPort));
        jj.put(Protocol.managementport.toString(), String.valueOf(serverManagementPort));
        return jj.toJSONString();
    }

    public String setCoordinatorMessage(String serverId, String serverAddress, Integer serverPort, Integer
            serverManagementPort) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.coordinator.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.address.toString(), serverAddress);
        jj.put(Protocol.port.toString(), String.valueOf(serverPort));
        jj.put(Protocol.managementport.toString(), String.valueOf(serverManagementPort));
        return jj.toJSONString();
    }

    public String answerVoteMessage(String suspectServerId, String vote, String votedBy){
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.answervote.toString());
        jj.put(Protocol.suspectserverid.toString(), suspectServerId);
        jj.put(Protocol.votedby.toString(), votedBy);
        jj.put(Protocol.vote.toString(), vote);
        return jj.toJSONString();
    }

    public String iAmUpMessage(String serverId, String serverAddress, Integer serverPort, Integer
            serverManagementPort) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.iamup.toString());
        jj.put(Protocol.serverid.toString(), serverId);
        jj.put(Protocol.address.toString(), serverAddress);
        jj.put(Protocol.port.toString(), String.valueOf(serverPort));
        jj.put(Protocol.managementport.toString(), String.valueOf(serverManagementPort));
        return jj.toJSONString();
    }

    public String viewMessage(String coordinatorId, String coordinatorAddress, Integer coordinatorPort, Integer
            coordinatorManagementPort) {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.viewelection.toString());
        jj.put(Protocol.currentcoordinatorid.toString(), coordinatorId);
        jj.put(Protocol.currentcoordinatoraddress.toString(), coordinatorAddress);
        jj.put(Protocol.currentcoordinatorport.toString(), String.valueOf(coordinatorPort));
        jj.put(Protocol.currentcoordinatormanagementport.toString(), String.valueOf(coordinatorManagementPort));
        return jj.toJSONString();
    }

    public String nominationMessage() {
        JSONObject jj = new JSONObject();
        jj.put(Protocol.type.toString(), Protocol.nominationelection.toString());
        return jj.toJSONString();
    }

}
