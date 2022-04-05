package chat.election;

import chat.improvements.FastBullyElection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@DisallowConcurrentExecution
public class FastBullyNominationMessageTimeoutFinalizer extends MessageTimeoutFinalizer {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (!interrupted.get()) {
            new FastBullyElection().stopElection(serverState.getServerInfo());
            new FastBullyElection()
                    .startElection(
                            serverState.getServerInfo(),
                            serverState.getCandidateServerInfoList(),
                            serverState.getElectionAnswerTimeout()
                    );

        }
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private static final Logger logger = LogManager.getLogger(FastBullyNominationMessageTimeoutFinalizer.class);
}
