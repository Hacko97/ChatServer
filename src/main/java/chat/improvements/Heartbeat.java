package chat.improvements;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import chat.server.ServerInfo;
import chat.service.JSONMessageBuilder;
import chat.service.ServerCommunication;
import chat.server.ServerState;

public class Heartbeat implements Job {

    private ServerState serverState = ServerState.getInstance();
    private JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();
    private ServerCommunication peerClient = new ServerCommunication();

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        if (null != serverState.getCoordinator()) {
            System.out.println("Current coordinator --> " + serverState.getCoordinator().getServerId());

        }

        for (ServerInfo serverInfo : serverState.getServerInfoList()) {
            String serverId = serverInfo.getServerId();
            String myServerId = serverState.getServerInfo().getServerId();
            if (serverId.equalsIgnoreCase(myServerId)) {
                continue;
            }

            boolean online = serverState.isOnline(serverInfo);
            if (!online) {
                Integer count = serverState.getAliveMap().get(serverId);
                if (count == null) {
                    serverState.getAliveMap().put(serverId, 1);
                } else {
                    serverState.getAliveMap().put(serverId, count + 1);
                }

                JobDataMap dataMap = context.getJobDetail().getJobDataMap();
                String aliveErrorFactor = dataMap.get("aliveErrorFactor").toString();

                count = serverState.getAliveMap().get(serverId);

                if (count > Integer.parseInt(aliveErrorFactor)) {
                    if (null != serverState.getCoordinator() && serverInfo.getServerId().equalsIgnoreCase(serverState
                            .getCoordinator().getServerId())) {
                        if (serverState.getIsFastBully()) {

                            new FastBullyElection().startElection(serverState.getServerInfo(),
                                    serverState.getCandidateServerInfoList(), serverState.getElectionAnswerTimeout());
                        }
                    }
                    peerClient.relayPeers(messageBuilder.notifyServerDownMessage(serverId));
                    System.out.println("Notify server " + serverId + " down --> Removing ");

                    serverState.removeServer(serverId);
                    serverState.removeRemoteChatRoomsByServerId(serverId);

                }
            }
        }
    }
}
