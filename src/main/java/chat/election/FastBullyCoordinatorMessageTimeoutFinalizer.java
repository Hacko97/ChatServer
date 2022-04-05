package chat.election;

import chat.improvements.FastBullyElection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import chat.server.ServerInfo;

@DisallowConcurrentExecution
public class FastBullyCoordinatorMessageTimeoutFinalizer extends MessageTimeoutFinalizer {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (!interrupted.get()) {
            FastBullyElection fastBullyElectionManagementService =
                    new FastBullyElection();

            try {

                ServerInfo topCandidate = serverState.getTopCandidate();

                if (null != topCandidate) {
                    fastBullyElectionManagementService.sendNominationMessage(topCandidate);
                    fastBullyElectionManagementService
                            .resetWaitingForCoordinatorMessageTimer(context, context.getTrigger().getKey(),
                                    serverState.getElectionCoordinatorTimeout());

                } else {
                    fastBullyElectionManagementService
                            .stopElection(serverState.getServerInfo());

                    fastBullyElectionManagementService.startElection(serverState.getServerInfo(), serverState
                            .getCandidateServerInfoList(), serverState.getElectionAnswerTimeout());

                }
            } catch (NullPointerException ne) {
                System.out.println(ne.getLocalizedMessage());
            }
        }
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private static final Logger logger = LogManager.getLogger(FastBullyCoordinatorMessageTimeoutFinalizer.class);
}
