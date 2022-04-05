package chat.server;

import chat.model.*;
import chat.service.ServerPriority;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerState {

    private static ServerState instance;


    private ConcurrentHashMap<String, Integer> heartbeatCountList;

    private ConcurrentMap<String, Integer> aliveMap;

    private ConcurrentMap<String, LocalChatRoom> localChatRooms;
    private ConcurrentMap<String, RemoteChatRoom> remoteChatRooms;
    private ConcurrentMap<String, UserInfo> connectedClients;

    private Set<String> lockedIdentities;
    private Set<String> lockedRoomIdentities;

    private ConcurrentMap<String, ServerInfo> serverInfoMap;
    private ConcurrentNavigableMap<String, ServerInfo> candidateServerInfoMap;
    private ConcurrentNavigableMap<String, ServerInfo> tempCandidateServerInfoMap;
    private Map<String, ServerInfo> subordinateServerInfoMap;
    private ServerInfo serverInfo;

    private ServerInfo coordinator;

    private AtomicBoolean stopRunning;

    private AtomicBoolean isFastBully;
    private AtomicBoolean ongoingElection;
    private AtomicBoolean answerMessageReceived;
    private AtomicBoolean viewMessageReceived;
    private Long electionAnswerTimeout;
    private Long electionCoordinatorTimeout;
    private Long electionNominationTimeout;

    private AtomicBoolean ongoingConsensus;

    private ServerState() {


        heartbeatCountList = new ConcurrentHashMap<>();
        aliveMap = new ConcurrentHashMap<>();

        connectedClients = new ConcurrentHashMap<>();
        localChatRooms = new ConcurrentHashMap<>();
        remoteChatRooms = new ConcurrentHashMap<>();
        lockedIdentities = new HashSet<>();
        lockedRoomIdentities = new HashSet<>();
        serverInfoMap = new ConcurrentHashMap<>();
        candidateServerInfoMap = new ConcurrentSkipListMap<>(new ServerPriority());
        tempCandidateServerInfoMap = new ConcurrentSkipListMap<>(new ServerPriority());
        subordinateServerInfoMap = new ConcurrentHashMap<>();
        isFastBully = new AtomicBoolean();
        ongoingElection = new AtomicBoolean(false);
        answerMessageReceived = new AtomicBoolean(false);
        viewMessageReceived = new AtomicBoolean(false);
        stopRunning = new AtomicBoolean(false);
        ongoingConsensus = new AtomicBoolean(false);
    }

    public static synchronized ServerState getInstance() {
        if (instance == null) {
            instance = new ServerState();
        }
        return instance;
    }

    public synchronized void initServerState(String serverId) {
        serverInfo = serverInfoMap.get(serverId);
    }

    public synchronized ServerInfo getServerInfoById(String serverId) {
        return serverInfoMap.get(serverId);
    }

    public synchronized ServerInfo getServerInfo() {
        return serverInfo;
    }

    public synchronized List<ServerInfo> getServerInfoList() {

        return new ArrayList<>(serverInfoMap.values());
    }

    public synchronized void initializeTemporaryCandidateMap() {
        tempCandidateServerInfoMap = new ConcurrentSkipListMap<>();
    }

    public synchronized ServerInfo getTopCandidate() {
        return tempCandidateServerInfoMap.pollFirstEntry().getValue();
    }

    public synchronized void resetTemporaryCandidateMap() {
        tempCandidateServerInfoMap = new ConcurrentSkipListMap<>();
    }

    public synchronized void addToTemporaryCandidateMap(ServerInfo serverInfo) {
        ServerInfo me = getServerInfo();
        if (null != serverInfo) {
            if (null != me) {
                if (new ServerPriority().compare(me.getServerId(), serverInfo.getServerId()) > 0) {
                    tempCandidateServerInfoMap.put(serverInfo.getServerId(), serverInfo);
                }
            }
        }
    }


    public synchronized List<ServerInfo> getCandidateServerInfoList() {
        return new ArrayList<>(candidateServerInfoMap.values());
    }

    public synchronized List<ServerInfo> getSubordinateServerInfoList() {
        return new ArrayList<>(subordinateServerInfoMap.values());
    }

    public synchronized void setServerInfoList(List<ServerInfo> serverInfoList) {
        for (ServerInfo serverInfo : serverInfoList) {
            addServer(serverInfo);
        }
    }

    public synchronized void addServer(ServerInfo serverInfo) {
        ServerInfo me = getServerInfo();
        if (null != serverInfo) {
            if (null != me) {
                if (new ServerPriority().compare(me.getServerId(), serverInfo.getServerId()) > 0) {
                    candidateServerInfoMap.put(serverInfo.getServerId(), serverInfo);
                } else if (new ServerPriority().compare(me.getServerId(), serverInfo.getServerId()) < 0) {
                    subordinateServerInfoMap.put(serverInfo.getServerId(), serverInfo);
                }
            }
            serverInfoMap.put(serverInfo.getServerId(), serverInfo);
        }

    }

    public synchronized void setupConnectedServers() {
        for (ServerInfo server : getServerInfoList()) {
            addServer(server);
        }
    }

    public synchronized void removeServer(String serverId) {
        serverInfoMap.remove(serverId);
    }

    public synchronized void removeServerInCountList(String serverId) {
        heartbeatCountList.remove(serverId);
    }







    public ConcurrentMap<String, Integer> getAliveMap() {
        return aliveMap;
    }


    public ConcurrentMap<String, UserInfo> getConnectedClients() {
        return connectedClients;
    }

    public ConcurrentMap<String, LocalChatRoom> getLocalChatRooms() {
        return localChatRooms;
    }

    public ConcurrentMap<String, RemoteChatRoom> getRemoteChatRooms() {
        return remoteChatRooms;
    }

    public boolean isUserExisted(String userId) {
        return connectedClients.containsKey(userId);
    }

    public boolean isRoomExistedGlobally(String roomId) {
        return localChatRooms.containsKey(roomId) || remoteChatRooms.containsKey(roomId);
    }

    public boolean isRoomExistedLocally(String roomId) {
        return localChatRooms.containsKey(roomId);
    }

    public boolean isRoomExistedRemotely(String roomId) {
        return remoteChatRooms.containsKey(roomId);
    }

    public void stopRunning(boolean state) {
        stopRunning.set(state);
    }

    public boolean isStopRunning() {
        return stopRunning.get();
    }


    public void removeRemoteChatRoomsByServerId(String serverId) {
        for (String entry : remoteChatRooms.keySet()) {
            RemoteChatRoom remoteChatRoomInfo = remoteChatRooms.get(entry);
            if (remoteChatRoomInfo.getManagingServer().equalsIgnoreCase(serverId)) {
                remoteChatRooms.remove(entry);
            }
        }
    }



    // synchronized

    public synchronized void lockIdentity(String identity) {
        lockedIdentities.add(identity);
    }

    public synchronized void unlockIdentity(String identity) {
        lockedIdentities.remove(identity);
    }

    public synchronized boolean isIdentityLocked(String identity) {
        return lockedIdentities.contains(identity);
    }

    public synchronized void lockRoomIdentity(String roomId) {
        lockedRoomIdentities.add(roomId);
    }

    public synchronized void unlockRoomIdentity(String roomId) {
        lockedRoomIdentities.remove(roomId);
    }

    public synchronized boolean isRoomIdLocked(String roomId) {
        return lockedRoomIdentities.contains(roomId);
    }

    public synchronized ServerInfo getCoordinator() {
        return coordinator;
    }

    public synchronized void setCoordinator(ServerInfo coordinator) {
        addServer(coordinator);
        this.coordinator = coordinator;
    }

    public boolean isOnline(ServerInfo serverInfo) {
        boolean online = true;
        try {
            InetSocketAddress address = new InetSocketAddress(serverInfo.getAddress(), serverInfo.getManagementPort());
            final int timeOut = (int) TimeUnit.SECONDS.toMillis(5);
            SocketFactory socketfactory = (SocketFactory) SocketFactory.getDefault();
            final Socket shortKet = (Socket) socketfactory.createSocket();
            shortKet.connect(address, timeOut);

            shortKet.close();
        } catch (IOException e) {

            online = false;
        }
        return online;
    }


    public synchronized Long getElectionAnswerTimeout() {
        return electionAnswerTimeout;
    }

    public synchronized void setElectionAnswerTimeout(Long electionAnswerTimeout) {
        this.electionAnswerTimeout = electionAnswerTimeout;
    }

    public synchronized Long getElectionCoordinatorTimeout() {
        return electionCoordinatorTimeout;
    }

    public synchronized void setElectionCoordinatorTimeout(Long electionCoordinatorTimeout) {
        this.electionCoordinatorTimeout = electionCoordinatorTimeout;
    }

    public synchronized Long getElectionNominationTimeout() {
        return electionNominationTimeout;
    }

    public synchronized void setElectionNominationTimeout(Long electionNominationTimeout) {
        this.electionNominationTimeout = electionNominationTimeout;
    }

    public boolean getIsFastBully() {
        return isFastBully.get();
    }

    public void setIsFastBully(boolean isFastBully) {
        this.isFastBully.set(isFastBully);
    }

    public void setOngoingElection(boolean ongoingElection) {
        this.ongoingElection.set(ongoingElection);
    }

    public boolean answerMessageReceived() {
        return answerMessageReceived.get();
    }

    public void setAnswerMessageReceived(boolean answerMessageReceived) {
        this.answerMessageReceived.set(answerMessageReceived);
    }

    public boolean viewMessageReceived() {
        return viewMessageReceived.get();
    }

    public void setViewMessageReceived(boolean viewMessageReceived) {
        this.viewMessageReceived.set(viewMessageReceived);
    }
}
