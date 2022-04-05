package chat.election;

import chat.improvements.FastBullyElection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import chat.server.ServerInfo;

@DisallowConcurrentExecution
public class FastBullyAnswerMessageTimeoutFinalizer extends MessageTimeoutFinalizer {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        FastBullyElection fastBullyElectionManagementService =
                new FastBullyElection();

        if (serverState.answerMessageReceived() || interrupted.get()) {
            ServerInfo topCandidate = serverState.getTopCandidate();
            fastBullyElectionManagementService.sendNominationMessage(topCandidate);
            System.out.println("Answer message received. Sending nomination to : " + topCandidate.getServerId());
            fastBullyElectionManagementService
                    .startWaitingForCoordinatorMessage(serverState.getElectionCoordinatorTimeout());
            serverState.setAnswerMessageReceived(false);
        } else {
            fastBullyElectionManagementService.sendCoordinatorMessage(serverState.getServerInfo(),
                    serverState.getSubordinateServerInfoList());

            fastBullyElectionManagementService.acceptNewCoordinator(serverState.getServerInfo());
            fastBullyElectionManagementService.stopElection(serverState.getServerInfo());
        }
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private static final Logger logger = LogManager.getLogger(FastBullyAnswerMessageTimeoutFinalizer.class);
}
