package chat.improvements;

import chat.election.FastBullyAnswerMessageTimeoutFinalizer;
import chat.election.FastBullyCoordinatorMessageTimeoutFinalizer;
import chat.election.FastBullyNominationMessageTimeoutFinalizer;
import chat.election.FastBullyViewMessageTimeoutFinalizer;
import chat.service.JSONMessageBuilder;
import chat.service.ServerCommunication;
import chat.service.Quartz;
import chat.server.ServerState;
import org.quartz.*;
import chat.server.ServerInfo;

import java.util.List;

public class FastBullyElection {

    protected JSONMessageBuilder jsonMessageBuilder;
    protected ServerCommunication peerClient;
    protected ServerState serverState;
    protected Scheduler scheduler;

    public static final String ELECTION_TRIGGER = "election_trigger".toUpperCase();

    public FastBullyElection() {
        this.jsonMessageBuilder = JSONMessageBuilder.getInstance();
        this.peerClient = new ServerCommunication();
        this.serverState = ServerState.getInstance();
        this.scheduler = Quartz.getInstance().getScheduler();
    }

    public void startElection(ServerInfo proposingCoordinator, List<ServerInfo> candidatesList, Long electionAnswerTimeout) {

        serverState.initializeTemporaryCandidateMap();
        serverState.setAnswerMessageReceived(false);
        serverState.setOngoingElection(true);

        System.out.println("Starting election...");
        String proposingCoordinatorServerId = proposingCoordinator.getServerId();
        String proposingCoordinatorAddress = proposingCoordinator.getAddress();
        Long proposingCoordinatorPort = Long.valueOf(proposingCoordinator.getPort());
        Long proposingCoordinatorManagementPort = Long.valueOf(proposingCoordinator.getManagementPort());
        String startElectionMessage = jsonMessageBuilder
                .startElectionMessage(proposingCoordinatorServerId, proposingCoordinatorAddress,
                        proposingCoordinatorPort, proposingCoordinatorManagementPort);
        peerClient.relaySelectedPeers(candidatesList, startElectionMessage);

        startWaitingForFastBullyAnswerMessage(electionAnswerTimeout);
    }

    public void startWaitingTimer(String groupId, Long timeout, JobDetail jobDetail) {
        try {


            if (scheduler.checkExists(jobDetail.getKey())) {

                System.out.println(String.format("Job get trigger again [%s]", jobDetail.getKey().getName()));
                scheduler.triggerJob(jobDetail.getKey());

            } else {
                SimpleTrigger simpleTrigger =
                        (SimpleTrigger) TriggerBuilder.newTrigger()
                                .withIdentity(ELECTION_TRIGGER, groupId)
                                .startAt(DateBuilder.futureDate(Math.toIntExact(timeout), DateBuilder.IntervalUnit.SECOND))
                                .build();

                scheduler.scheduleJob(jobDetail, simpleTrigger);
            }

        } catch (ObjectAlreadyExistsException oe) {

            System.out.println(oe.getMessage());

            try {

                System.out.println(String.format("Job get trigger again [%s]", jobDetail.getKey().getName()));
                scheduler.triggerJob(jobDetail.getKey());

            } catch (SchedulerException e) {
                e.printStackTrace();
            }

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public void startWaitingForFastBullyAnswerMessage(Long timeout) {
        JobDetail answerMsgTimeoutJob =
                JobBuilder.newJob(FastBullyAnswerMessageTimeoutFinalizer.class).withIdentity
                        ("answer_msg_timeout_job", "group_fast_bully").build();
        startWaitingTimer("group_fast_bully", timeout, answerMsgTimeoutJob);
    }


    public void resetWaitingForCoordinatorMessageTimer(JobExecutionContext context, TriggerKey triggerKey, Long timeout) {
        try {
            JobDetail jobDetail = context.getJobDetail();
            if (scheduler.checkExists(jobDetail.getKey())) {

                System.out.println(String.format("Job get trigger again [%s]", jobDetail.getKey().getName()));
                scheduler.triggerJob(jobDetail.getKey());

            } else {

                Trigger simpleTrigger = TriggerBuilder.newTrigger()
                        .withIdentity("election_trigger", "group_fast_bully")
                        .startAt(DateBuilder.futureDate(Math.toIntExact(timeout), DateBuilder.IntervalUnit.SECOND))
                        .build();
                context.getScheduler().rescheduleJob(triggerKey, simpleTrigger);
            }

        } catch (ObjectAlreadyExistsException oe) {
            System.out.println(oe.getLocalizedMessage());

            try {

                JobDetail jobDetail = context.getJobDetail();
                System.out.println(String.format("Job get trigger again [%s]", jobDetail.getKey().getName()));

                scheduler.triggerJob(jobDetail.getKey());

            } catch (SchedulerException e) {
                e.printStackTrace();
            }

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }


    public void replyAnswerForElectionMessage(ServerInfo requestingCandidate, ServerInfo me) {
        System.out.println("Replying answer for the election start message from : " + requestingCandidate.getServerId());
        String electionAnswerMessage = jsonMessageBuilder
                .electionAnswerMessage(me.getServerId(), me.getAddress(), me.getPort(), me.getManagementPort());
        peerClient.commPeerOneWay(requestingCandidate, electionAnswerMessage);
    }

    public void acceptNewCoordinator(ServerInfo newCoordinator) {
        serverState.setCoordinator(newCoordinator);
        serverState.setOngoingElection(false);
        serverState.setViewMessageReceived(false);
        serverState.setAnswerMessageReceived(false);
        System.out.println("Accepting new coordinator --> " + newCoordinator.getServerId());
    }

    public void startWaitingForNominationOrCoordinationMessage(Long timeout) {
        JobDetail coordinatorMsgTimeoutJob =
                JobBuilder.newJob(FastBullyNominationMessageTimeoutFinalizer.class).withIdentity
                        ("coordinator_or_nomination_msg_timeout_job", "group_fast_bully").build();
        startWaitingTimer("group_fast_bully", timeout, coordinatorMsgTimeoutJob);
    }

    public void startWaitingForCoordinatorMessage(Long timeout) {
        JobDetail coordinatorMsgTimeoutJob =
                JobBuilder.newJob(FastBullyCoordinatorMessageTimeoutFinalizer.class).withIdentity
                        ("coordinator_msg_timeout_job", "group_fast_bully").build();
        startWaitingTimer("group_fast_bully", timeout, coordinatorMsgTimeoutJob);
    }

    public void startWaitingForViewMessage(Long electionAnswerTimeout) throws SchedulerException {
        JobDetail coordinatorMsgTimeoutJob =
                JobBuilder.newJob(FastBullyViewMessageTimeoutFinalizer.class).withIdentity
                        ("view_msg_timeout_job", "group_fast_bully").build();
        startWaitingTimer("group_fast_bully", electionAnswerTimeout, coordinatorMsgTimeoutJob);
    }

    public void stopElection(ServerInfo stoppingServer) {

        serverState.resetTemporaryCandidateMap();
        serverState.setOngoingElection(false);

        stopWaitingForAnswerMessage();
        stopWaitingForCoordinatorMessage();
        stopWaitingForNominationMessage();
        stopWaitingForViewMessage();
    }

    public void stopWaitingTimer(JobKey jobKey) {
        try {
            if (scheduler.checkExists(jobKey)) {
                scheduler.interrupt(jobKey);
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public void stopWaitingForAnswerMessage() {
        JobKey answerMsgTimeoutJobKey = new JobKey("answer_msg_timeout_job", "group_fast_bully");
        stopWaitingTimer(answerMsgTimeoutJobKey);
    }

    public void stopWaitingForNominationMessage() {
        JobKey answerMsgTimeoutJobKey = new JobKey("coordinator_or_nomination_msg_timeout_job", "group_fast_bully");
        stopWaitingTimer(answerMsgTimeoutJobKey);
    }

    public void stopWaitingForCoordinatorMessage() {
        JobKey coordinatorMsgTimeoutJobKey = new JobKey("coordinator_msg_timeout_job", "group_fast_bully");
        stopWaitingTimer(coordinatorMsgTimeoutJobKey);
    }

    public void stopWaitingForViewMessage() {
        JobKey viewMsgTimeoutJobKey = new JobKey("view_msg_timeout_job", "group_fast_bully");
        stopWaitingTimer(viewMsgTimeoutJobKey);
    }

    public void sendIamUpMessage(ServerInfo serverInfo, List<ServerInfo> serverInfoList) {
        String iAmUpMessage = jsonMessageBuilder.iAmUpMessage(serverInfo.getServerId(), serverInfo.getAddress(),
                serverInfo.getPort(), serverInfo.getManagementPort());
        peerClient.relaySelectedPeers(serverInfoList, iAmUpMessage);
    }

    public void sendViewMessage(ServerInfo sender, ServerInfo coordinator) {
        if (null == coordinator) {
            coordinator = sender;
        }
        String viewMessage = jsonMessageBuilder.viewMessage(coordinator.getServerId(), coordinator.getAddress(),
                coordinator.getPort(), coordinator.getManagementPort());
        peerClient.commPeerOneWay(sender, viewMessage);
    }

    public void sendNominationMessage(ServerInfo topCandidate) {
        peerClient.commPeerOneWay(topCandidate, jsonMessageBuilder.nominationMessage());
    }

    public void sendCoordinatorMessage(ServerInfo coordinator, List<ServerInfo> subordinateServerInfoList) {
        String coordinatorMessage = jsonMessageBuilder.setCoordinatorMessage(coordinator
                .getServerId(), coordinator.getAddress(), coordinator.getPort(), coordinator.getManagementPort());
        peerClient.relaySelectedPeers(subordinateServerInfoList, coordinatorMessage);
    }

}
