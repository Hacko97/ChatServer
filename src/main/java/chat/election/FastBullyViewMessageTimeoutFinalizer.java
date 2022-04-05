package chat.election;

import chat.improvements.FastBullyElection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import chat.server.ServerInfo;

@DisallowConcurrentExecution
public class FastBullyViewMessageTimeoutFinalizer extends MessageTimeoutFinalizer {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        FastBullyElection fastBullyElectionManagementService =
                new FastBullyElection();
        ServerInfo myServerInfo = serverState.getServerInfo();
        if (!interrupted.get() && !serverState.viewMessageReceived()) {
            fastBullyElectionManagementService.stopElection(myServerInfo);
            if (null == serverState.getCoordinator()) {
                fastBullyElectionManagementService.sendCoordinatorMessage(myServerInfo, serverState
                        .getSubordinateServerInfoList());
                fastBullyElectionManagementService.acceptNewCoordinator(myServerInfo);
            }

            serverState.setViewMessageReceived(false);
        }
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private static final Logger logger = LogManager.getLogger(FastBullyViewMessageTimeoutFinalizer.class);
}
