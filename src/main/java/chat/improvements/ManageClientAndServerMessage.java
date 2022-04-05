package chat.improvements;

import chat.model.*;
import chat.server.ServerInfo;
import chat.service.*;
import org.json.simple.JSONObject;
import chat.server.ServerState;

import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class ManageClientAndServerMessage {

    @Deprecated
    public static String newMessageHandler(JSONObject jsonMessage, Runnable connection) {

        if (connection instanceof ClientConnection) newClientMessageHandler(jsonMessage, connection);

        if (connection instanceof ManagementConnection) newManagementMessageHandler(jsonMessage, connection);

       return "";
        //logger.warn("BlackHoleHandler: ");
    }

    public static String newClientMessageHandler(JSONObject jsonMessage, Runnable connection) {

        if (connection instanceof ManagementConnection) {
            String s = "";
            return s;
        }
        String type = (String) jsonMessage.get(Protocol.type.toString());
        ClientConnection clientConnection= (ClientConnection) connection;
        BlockingQueue<Message> messageQueue = clientConnection.getMessageQueue();
        JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();
        ServerState serverState = ServerState.getInstance();
        UserInfo userInfo;
        Socket clientSocket = clientConnection.getClientSocket();
        ServerInfo serverInfo = serverState.getServerInfo();
        String mainHall = "MainHall-" + serverInfo.getServerId();
        ServerCommunication peerClient = new ServerCommunication();

        if (jsonMessage == null) {

            userInfo = clientConnection.getUserInfo();
            if (userInfo != null) {

                String former = userInfo.getCurrentChatRoom();

                serverState.getLocalChatRooms().get(former).removeMember(userInfo.getIdentity());

                if (userInfo.isRoomOwner()) {

                    LocalChatRoom deletingRoom = serverState.getLocalChatRooms().get(former);
                    serverState.getLocalChatRooms().get(mainHall).getMembers().addAll(deletingRoom.getMembers());
                    for (String member : deletingRoom.getMembers()) {
                        UserInfo client = serverState.getConnectedClients().get(member);
                        if (client.getIdentity().equalsIgnoreCase(userInfo.getIdentity())) continue;

                        client.setCurrentChatRoom(mainHall);
                        String msg = messageBuilder.roomChange(deletingRoom.getChatRoomId(), mainHall, client.getIdentity());

                        Message msg2 = new Message(false, msg);

                        Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                        connectedClients.values().stream()
                                .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(deletingRoom.getChatRoomId()))
                                .forEach(client2 -> {
                                    client2.getManagingThread().getMessageQueue().add(msg2);
                                });



                        connectedClients.values().stream()
                                .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                                .forEach(client2 -> {
                                    client2.getManagingThread().getMessageQueue().add(msg2);
                                });

                    }

                    serverState.getLocalChatRooms().remove(deletingRoom.getChatRoomId());

                    peerClient.relayPeers(messageBuilder.deleteRoomPeers(deletingRoom.getChatRoomId()));
                }

                serverState.getConnectedClients().remove(userInfo.getIdentity());

                if (!clientConnection.isRouted()) {
                    //String former = userInfo.getCurrentChatRoom();

                    Message msg = new Message(false, messageBuilder.roomChange(former, "", userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    UserInfo finalUserInfo5 = userInfo;
                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(former))
                            .filter(client -> !client.getIdentity().equalsIgnoreCase(finalUserInfo5.getIdentity()))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });
                }
            }
        }




        if (type.equalsIgnoreCase(Protocol.listserver.toString())) {

            messageQueue.add(new Message(false, messageBuilder.listServers()));

        }

        if (type.equalsIgnoreCase(Protocol.movejoin.toString())) {


            String joiningRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            String former = (String) jsonMessage.get(Protocol.former.toString());
            String identity = (String) jsonMessage.get(Protocol.identity.toString());
            boolean roomExistedLocally = serverState.isRoomExistedLocally(joiningRoomId);

            userInfo = new UserInfo();
            userInfo.setIdentity(identity);
            userInfo.setManagingThread(clientConnection);
            userInfo.setSocket(clientSocket);

            clientConnection.setUserInfo(userInfo);

            String roomId;
            if (roomExistedLocally) {
                roomId = joiningRoomId;
            } else {
                roomId = mainHall;
            }
            userInfo.setCurrentChatRoom(roomId);
            serverState.getConnectedClients().put(identity, userInfo);
            serverState.getLocalChatRooms().get(roomId).addMember(identity);



            clientConnection.write(messageBuilder.serverChange("true", serverInfo.getServerId()));

            Message msg = new Message(false, messageBuilder.roomChange(former, roomId, userInfo.getIdentity()));

            Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

            connectedClients.values().stream()
                    .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase( roomId))
                    .forEach(client -> {
                        client.getManagingThread().getMessageQueue().add(msg);
                    });
        }

        if (type.equalsIgnoreCase(Protocol.newidentity.toString())) {
            String requestIdentity = (String) jsonMessage.get(Protocol.identity.toString());

            boolean isUserExisted = serverState.isUserExisted(requestIdentity);
            boolean isUserIdValid = Utilities.isIdValid(requestIdentity);

            if (isUserExisted || !isUserIdValid) {
                messageQueue.add(new Message(false, messageBuilder.newIdentityResp("false")));
            } else {

                boolean canLock = peerClient.canPeersLockId(messageBuilder.lockIdentity(requestIdentity));

                if (canLock) {
                    userInfo = new UserInfo();
                    userInfo.setIdentity(requestIdentity);
                    userInfo.setCurrentChatRoom(mainHall);
                    userInfo.setManagingThread(clientConnection);
                    userInfo.setSocket(clientSocket);

                    clientConnection.setUserInfo(userInfo);

                    serverState.getConnectedClients().put(requestIdentity, userInfo);
                    serverState.getLocalChatRooms().get(mainHall).addMember(requestIdentity);




                    messageQueue.add(new Message(false, messageBuilder.newIdentityResp("true")));

                    Message msg = new Message(false, messageBuilder.roomChange("", mainHall, userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });
                } else {
                    messageQueue.add(new Message(false, messageBuilder.newIdentityResp("false")));
                }

                peerClient.relayPeers(messageBuilder.releaseIdentity(requestIdentity));
            }
        }

        if (type.equalsIgnoreCase(Protocol.list.toString())) {
            messageQueue.add(new Message(false, messageBuilder.listRooms()));
        }

        if (type.equalsIgnoreCase(Protocol.who.toString())) {
            userInfo = clientConnection.getUserInfo();
            messageQueue.add(new Message(false, messageBuilder.whoByRoom(userInfo.getCurrentChatRoom())));

        }

        if (type.equalsIgnoreCase(Protocol.createroom.toString())) {

            String requestRoomId = (String) jsonMessage.get(Protocol.roomid.toString());

            boolean isRoomExisted = serverState.isRoomExistedGlobally(requestRoomId);
            boolean hasRoomAlreadyLocked = serverState.isRoomIdLocked(requestRoomId);
            boolean isRoomIdValid = Utilities.isIdValid(requestRoomId);
            userInfo = clientConnection.getUserInfo();
            if (userInfo.isRoomOwner() || hasRoomAlreadyLocked || isRoomExisted || !isRoomIdValid) {
                clientConnection.write(messageBuilder.createRoomResp(requestRoomId, "false"));
            } else {

                boolean canLock = peerClient.canPeersLockId(messageBuilder.lockRoom(requestRoomId));
                if (canLock) {

                    peerClient.relayPeers(messageBuilder.releaseRoom(requestRoomId, "true"));
                    LocalChatRoom newRoom = new LocalChatRoom();
                    newRoom.setChatRoomId(requestRoomId);
                    newRoom.setOwner(userInfo.getIdentity());
                    newRoom.addMember(userInfo.getIdentity());
                    serverState.getLocalChatRooms().put(requestRoomId, newRoom);

                    String former = userInfo.getCurrentChatRoom();
                    serverState.getLocalChatRooms().get(former).removeMember(userInfo.getIdentity());

                    userInfo.setCurrentChatRoom(requestRoomId);
                    userInfo.setRoomOwner(true);

                    clientConnection.write(messageBuilder.createRoomResp(requestRoomId, "true"));
                    clientConnection.write(messageBuilder.roomChange(former, userInfo.getCurrentChatRoom(), userInfo.getIdentity()));

                    Message msg = new Message(false, messageBuilder.roomChange(former, userInfo.getCurrentChatRoom(), userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(former))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });

                } else {
                    peerClient.relayPeers(messageBuilder.releaseRoom(requestRoomId, "false"));
                    clientConnection.write(messageBuilder.createRoomResp(requestRoomId, "false"));
                }
            }
        }

        if (type.equalsIgnoreCase(Protocol.joinroom.toString())) {

            String joiningRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            userInfo = clientConnection.getUserInfo();
            boolean roomExistedGlobally = serverState.isRoomExistedGlobally(joiningRoomId);
            boolean isTheSameRoom = userInfo.getCurrentChatRoom().equalsIgnoreCase(joiningRoomId);
            if (userInfo.isRoomOwner() || !roomExistedGlobally || isTheSameRoom) {
                messageQueue.add(new Message(false, messageBuilder.roomChange(joiningRoomId, joiningRoomId, userInfo.getIdentity())));
            } else {

                boolean roomExistedLocally = serverState.isRoomExistedLocally(joiningRoomId);
                boolean roomExistedRemotely = serverState.isRoomExistedRemotely(joiningRoomId);

                String former = userInfo.getCurrentChatRoom();

                if (roomExistedLocally) {
                    userInfo.setCurrentChatRoom(joiningRoomId);

                    serverState.getLocalChatRooms().get(joiningRoomId).addMember(userInfo.getIdentity());


                    Message msg = new Message(false, messageBuilder.roomChange(former, joiningRoomId, userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    UserInfo finalUserInfo = userInfo;
                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(former))
                            .filter(client -> !client.getIdentity().equalsIgnoreCase( finalUserInfo.getIdentity()))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });

                    Message msg2 = new Message(false, messageBuilder.roomChange(former, joiningRoomId, userInfo.getIdentity()));




                    UserInfo finalUserInfo1 = userInfo;
                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(joiningRoomId))
                            .filter(client -> !client.getIdentity().equalsIgnoreCase( finalUserInfo1.getIdentity()))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg2);
                            });
                    messageQueue.add(new Message(false, messageBuilder.roomChange(former, joiningRoomId, userInfo.getIdentity())));
                }

                if (roomExistedRemotely) {
                    RemoteChatRoom remoteChatRoomInfo = serverState.getRemoteChatRooms().get(joiningRoomId);
                    ServerInfo server = serverState.getServerInfoById(remoteChatRoomInfo.getManagingServer());

                    messageQueue.add(new Message(false, messageBuilder.route(joiningRoomId, server.getAddress(), server.getPort())));

                    clientConnection.setRouted(true);

                    Message msg = new Message(false, messageBuilder.roomChange(former, joiningRoomId, userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(former))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });
                }

                serverState.getLocalChatRooms().get(former).removeMember(userInfo.getIdentity());
            }
        }

        if (type.equalsIgnoreCase(Protocol.message.toString())) {

            userInfo = clientConnection.getUserInfo();
            String content = (String) jsonMessage.get(Protocol.content.toString());

            Message msg = new Message(false, messageBuilder.message(userInfo.getIdentity(), content));

            Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

            UserInfo finalUserInfo2 = userInfo;
            connectedClients.values().stream()
                    .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(finalUserInfo2.getCurrentChatRoom()))
                    .forEach(client -> {
                        client.getManagingThread().getMessageQueue().add(msg);
                    });
        }

        if (type.equalsIgnoreCase(Protocol.quit.toString())) {

            userInfo = clientConnection.getUserInfo();

            String former = userInfo.getCurrentChatRoom();


            serverState.getLocalChatRooms().get(former).removeMember(userInfo.getIdentity());

            if (userInfo.isRoomOwner()) {

                LocalChatRoom deletingRoom = serverState.getLocalChatRooms().get(former);
                serverState.getLocalChatRooms().get(mainHall).getMembers().addAll(deletingRoom.getMembers());
                for (String member : deletingRoom.getMembers()) {
                    UserInfo client = serverState.getConnectedClients().get(member);
                    if (client.getIdentity().equalsIgnoreCase(userInfo.getIdentity())) continue;

                    client.setCurrentChatRoom(mainHall);
                    String msg = messageBuilder.roomChange(deletingRoom.getChatRoomId(), mainHall, client.getIdentity());

                    Message msg2 = new Message(false, msg);

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    connectedClients.values().stream()
                            .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(deletingRoom.getChatRoomId()))
                            .forEach(client2 -> {
                                client2.getManagingThread().getMessageQueue().add(msg2);
                            });



                    connectedClients.values().stream()
                            .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                            .forEach(client2 -> {
                                client2.getManagingThread().getMessageQueue().add(msg2);
                            });

                }

                serverState.getLocalChatRooms().remove(deletingRoom.getChatRoomId());

                peerClient.relayPeers(messageBuilder.deleteRoomPeers(deletingRoom.getChatRoomId()));
            }

            serverState.getConnectedClients().remove(userInfo.getIdentity());

            if (userInfo.isRoomOwner()) {

                Message msg = new Message(false, messageBuilder.roomChange(former, "", userInfo.getIdentity()));

                Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                UserInfo finalUserInfo3 = userInfo;
                connectedClients.values().stream()
                        .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                        .filter(client -> !client.getIdentity().equalsIgnoreCase(finalUserInfo3.getIdentity()))
                        .forEach(client -> {
                            client.getManagingThread().getMessageQueue().add(msg);
                        });
            } else {
                Message msg = new Message(false, messageBuilder.roomChange(former, "", userInfo.getIdentity()));

                Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                UserInfo finalUserInfo4 = userInfo;
                connectedClients.values().stream()
                        .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(former))
                        .filter(client -> !client.getIdentity().equalsIgnoreCase(finalUserInfo4.getIdentity()))
                        .forEach(client -> {
                            client.getManagingThread().getMessageQueue().add(msg);
                        });
            }

            clientConnection.write(messageBuilder.roomChange(former, "", userInfo.getIdentity()));
        }

        if (type.equalsIgnoreCase(Protocol.deleteroom.toString())) {
            userInfo = clientConnection.getUserInfo();

            String deleteRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            boolean roomExistedLocally = serverState.isRoomExistedLocally(deleteRoomId);
            if (roomExistedLocally) {
                LocalChatRoom deletingRoom = serverState.getLocalChatRooms().get(deleteRoomId);
                if (deletingRoom.getOwner().equalsIgnoreCase(userInfo.getIdentity())) {

                    userInfo.setRoomOwner(false);
                    userInfo.setCurrentChatRoom(mainHall);


                    serverState.getLocalChatRooms().get(mainHall).getMembers().addAll(deletingRoom.getMembers());
                    for (String member : deletingRoom.getMembers()) {
                        UserInfo client = serverState.getConnectedClients().get(member);
                        if (client.getIdentity().equalsIgnoreCase(userInfo.getIdentity())) continue;

                        client.setCurrentChatRoom(mainHall);
                        String msg = messageBuilder.roomChange(deletingRoom.getChatRoomId(), mainHall, client.getIdentity());
                        Message msg2 = new Message(false, msg);

                        Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                        connectedClients.values().stream()
                                .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(deletingRoom.getChatRoomId()))
                                .forEach(client2 -> {
                                    client2.getManagingThread().getMessageQueue().add(msg2);
                                });

                        connectedClients.values().stream()
                                .filter(client2 -> client2.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                                .forEach(client2 -> {
                                    client2.getManagingThread().getMessageQueue().add(msg2);
                                });
                    }

                    serverState.getLocalChatRooms().remove(deletingRoom.getChatRoomId());

                    peerClient.relayPeers(messageBuilder.deleteRoomPeers(deletingRoom.getChatRoomId()));


                    Message msg = new Message(false, messageBuilder.roomChange(deleteRoomId, mainHall, userInfo.getIdentity()));

                    Map<String, UserInfo> connectedClients = serverState.getConnectedClients();

                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(deleteRoomId))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg);
                            });
                    Message msg2 = new Message(false, messageBuilder.roomChange(deleteRoomId, mainHall, userInfo.getIdentity()));


                    connectedClients.values().stream()
                            .filter(client -> client.getCurrentChatRoom().equalsIgnoreCase(mainHall))
                            .forEach(client -> {
                                client.getManagingThread().getMessageQueue().add(msg2);
                            });

                    messageQueue.add(new Message(false, messageBuilder.deleteRoom(deleteRoomId, "true")));
                } else {
                    messageQueue.add(new Message(false, messageBuilder.deleteRoom(deleteRoomId, "false")));
                }
            } else {
                messageQueue.add(new Message(false, messageBuilder.deleteRoom(deleteRoomId, "false")));
            }
        }
return "";
    }

    public static String  newManagementMessageHandler(JSONObject jsonMessage, Runnable connection) {

        if (connection instanceof ClientConnection) {
            return "";
        }

        String type = (String) jsonMessage.get(Protocol.type.toString());

        if (type.equalsIgnoreCase(Protocol.lockidentity.toString())) {
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();
            JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();

            String requestUserId = (String) jsonMessage.get(Protocol.identity.toString());
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());
            String lok = serverId.concat(requestUserId);

            ServerState serverState = ServerState.getInstance();
            boolean isUserExisted = serverState.isUserExisted(requestUserId);
            boolean isUserLocked = serverState.isIdentityLocked(lok);

            if (isUserExisted || isUserLocked) {
                messageQueue.add(new Message(false, messageBuilder.lockIdentity(serverId, requestUserId, "false")));
            } else {
                serverState.lockIdentity(lok);
                messageQueue.add(new Message(false, messageBuilder.lockIdentity(serverId, requestUserId, "true")));
            }
        }


        if (type.equalsIgnoreCase(Protocol.releaseidentity.toString())) {
            ServerState serverState = ServerState.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();
            String requestUserId = (String) jsonMessage.get(Protocol.identity.toString());
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());
            String lok = serverId.concat(requestUserId);
            serverState.unlockIdentity(lok);
            messageQueue.add(new Message(false, "exit"));
        }

        if (type.equalsIgnoreCase(Protocol.lockroomid.toString())) {

            ServerState serverState = ServerState.getInstance();
            JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();

            String requestRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());

            boolean locked = serverState.isRoomIdLocked(requestRoomId);
            Set<String> existingRooms = serverState.getLocalChatRooms().keySet();
            boolean existed = existingRooms.contains(requestRoomId);
            if (locked || existed) { // deny lock
                messageQueue.add(new Message(false, messageBuilder.lockRoom(serverId, requestRoomId, "false")));
            } else { // approve lock
                serverState.lockRoomIdentity(requestRoomId);
                messageQueue.add(new Message(false, messageBuilder.lockRoom(serverId, requestRoomId, "true")));
            }
        }

        if (type.equalsIgnoreCase(Protocol.releaseroomid.toString())) {
            ServerState serverState = ServerState.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();


            String requestRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());
            String approved = (String) jsonMessage.get(Protocol.approved.toString());

            if (approved.equalsIgnoreCase("true")) {

                serverState.unlockRoomIdentity(requestRoomId);

                RemoteChatRoom remoteChatRoomInfo = new RemoteChatRoom();
                remoteChatRoomInfo.setChatRoomId(requestRoomId);
                remoteChatRoomInfo.setManagingServer(serverId);
                serverState.getRemoteChatRooms().put(requestRoomId, remoteChatRoomInfo);
            } else {

            }

            messageQueue.add(new Message(false, "exit"));
        }

        if (type.equalsIgnoreCase(Protocol.deleteroom.toString())) {
            ServerState serverState = ServerState.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();

            String deletingRoomId = (String) jsonMessage.get(Protocol.roomid.toString());
            serverState.getRemoteChatRooms().remove(deletingRoomId);
            messageQueue.add(new Message(false, "exit"));
        }


        if (type.equalsIgnoreCase(Protocol.serverup.toString())) {
            ServerState serverState = ServerState.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());
            String address = (String) jsonMessage.get(Protocol.address.toString());
            Long port = (Long) jsonMessage.get(Protocol.port.toString());
            Long managementPort = (Long) jsonMessage.get(Protocol.managementport.toString());

            ServerInfo serverInfo = new ServerInfo();
            serverInfo.setAddress(address);
            serverInfo.setServerId(serverId);
            serverInfo.setPort(Math.toIntExact(port));
            serverInfo.setManagementPort(Math.toIntExact(managementPort));

            serverState.addServer(serverInfo);

            messageQueue.add(new Message(false, "exit"));
        }

        if (type.equalsIgnoreCase(Protocol.notifyserverdown.toString())) {
            String serverId = (String) jsonMessage.get(Protocol.serverid.toString());

            System.out.println("Downtime notification received. Removing server --> " + serverId);
            ServerState serverState = ServerState.getInstance();
            ManagementConnection managementConnection = (ManagementConnection) connection;
            BlockingQueue<Message> messageQueue = managementConnection.getMessageQueue();
            serverState.removeServer(serverId);
            serverState.removeRemoteChatRoomsByServerId(serverId);

            serverState.removeServerInCountList(serverId);


            messageQueue.add(new Message(false, "exit"));
        }

//fastbully messages handle
        if (type.equalsIgnoreCase(Protocol.startelection.toString())) {


            String potentialCandidateId = (String) jsonMessage.get(Protocol.serverid.toString());

            String potentialCandidateAddress = (String) jsonMessage.get(Protocol.address.toString());

            Integer potentialCandidatePort = Integer.parseInt((String) jsonMessage.get(Protocol.port.toString()));

            Integer potentialCandidateManagementPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.managementport.toString()));

            ServerInfo potentialCandidate =
                    new ServerInfo(potentialCandidateId, potentialCandidateAddress, potentialCandidatePort,
                            potentialCandidateManagementPort);

            FastBullyElection fastBullyElectionManagementService =
                    new FastBullyElection();

            ServerState serverState = ServerState.getInstance();
            fastBullyElectionManagementService
                    .replyAnswerForElectionMessage(potentialCandidate, serverState.getServerInfo());

            fastBullyElectionManagementService
                    .startWaitingForNominationOrCoordinationMessage(serverState.getElectionNominationTimeout());

        }

        if (type.equalsIgnoreCase(Protocol.answerelection.toString())) {

            ServerState serverState = ServerState.getInstance();
            serverState.setAnswerMessageReceived(true);
            String potentialCandidateId = (String) jsonMessage.get(Protocol.serverid.toString());
            String potentialCandidateAddress = (String) jsonMessage.get(Protocol.address.toString());
            Integer potentialCandidatePort = Integer.parseInt((String) jsonMessage.get(Protocol.port.toString()));
            Integer potentialCandidateManagementPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.managementport.toString()));
            ServerInfo potentialCandidate =
                    new ServerInfo(potentialCandidateId, potentialCandidateAddress, potentialCandidatePort,
                            potentialCandidateManagementPort);

            serverState.addToTemporaryCandidateMap(potentialCandidate);

        }

        if (type.equalsIgnoreCase(Protocol.coordinator.toString())) {
            System.out.println("Received coordinator from : " + jsonMessage.get(Protocol.serverid.toString()));

            ServerState serverState = ServerState.getInstance();
            new FastBullyElection().stopElection(serverState.getServerInfo());

            String newCoordinatorId = (String) jsonMessage.get(Protocol.serverid.toString());
            String newCoordinatorAddress = (String) jsonMessage.get(Protocol.address.toString());
            Integer newCoordinatorPort = Integer.parseInt((String) jsonMessage.get(Protocol.port.toString()));
            Integer newCoordinatorManagementPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.managementport.toString()));
            ServerInfo newCoordinator = new ServerInfo(newCoordinatorId, newCoordinatorAddress, newCoordinatorPort,
                    newCoordinatorManagementPort);
            new FastBullyElection().acceptNewCoordinator(newCoordinator);
        }

        if (type.equalsIgnoreCase(Protocol.viewelection.toString())) {

            ServerState serverState = ServerState.getInstance();
            serverState.setViewMessageReceived(true);

            String currentCoordinatorId = (String) jsonMessage.get(Protocol.currentcoordinatorid.toString());
            String coordinatorAddress = (String) jsonMessage.get(Protocol.currentcoordinatoraddress.toString());
            Integer coordinatorPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.currentcoordinatorport.toString()));
            Integer coordinatorManagementPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.currentcoordinatormanagementport.toString()));
            ServerInfo currentCoordinator = new ServerInfo(currentCoordinatorId, coordinatorAddress, coordinatorPort,
                    coordinatorManagementPort);

            serverState.addToTemporaryCandidateMap(currentCoordinator);

            FastBullyElection fastBullyElectionManagementService =
                    new FastBullyElection();

            ServerInfo myServerInfo = serverState.getServerInfo();
            String myServerId = myServerInfo.getServerId();


            if (new ServerPriority().compare(myServerId, currentCoordinatorId) < 0) {

                fastBullyElectionManagementService.sendCoordinatorMessage(myServerInfo,
                        serverState.getSubordinateServerInfoList());
                fastBullyElectionManagementService.acceptNewCoordinator(myServerInfo);

            } else if (new ServerPriority().compare(myServerId, currentCoordinatorId) > 0) {

                fastBullyElectionManagementService.acceptNewCoordinator(currentCoordinator);

            } else {

                fastBullyElectionManagementService
                        .sendCoordinatorMessage(myServerInfo, serverState.getSubordinateServerInfoList());

                fastBullyElectionManagementService.acceptNewCoordinator(myServerInfo);
            }

            fastBullyElectionManagementService.stopWaitingForViewMessage();
        }

        if (type.equalsIgnoreCase(Protocol.nominationelection.toString())) {
            ServerState serverState = ServerState.getInstance();

            FastBullyElection fastBullyElectionManagementService =
                    new FastBullyElection();



            fastBullyElectionManagementService.sendCoordinatorMessage(
                    serverState.getServerInfo(),
                    serverState.getSubordinateServerInfoList());

            fastBullyElectionManagementService.acceptNewCoordinator(serverState.getServerInfo());


            fastBullyElectionManagementService.stopElection(serverState.getServerInfo());
        }

        if (type.equalsIgnoreCase(Protocol.iamup.toString())) {
            String senderId = (String) jsonMessage.get(Protocol.serverid.toString());
            String senderAddress = (String) jsonMessage.get(Protocol.address.toString());
            Integer senderPort = Integer.parseInt((String) jsonMessage.get(Protocol.port.toString()));
            Integer senderManagementPort =
                    Integer.parseInt((String) jsonMessage.get(Protocol.managementport.toString()));
            ServerInfo sender = new ServerInfo(senderId, senderAddress, senderPort, senderManagementPort);
            ServerState serverState;
            serverState = ServerState.getInstance();

            ServerInfo coordinator = serverState.getCoordinator();

            serverState.addToTemporaryCandidateMap(sender);
            new FastBullyElection().sendViewMessage(sender, coordinator);
        }



        return "";
    }

}
