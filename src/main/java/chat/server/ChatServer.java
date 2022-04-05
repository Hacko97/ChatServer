package chat.server;

import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.quartz.*;
import chat.model.Protocol;
import chat.improvements.Heartbeat;
import chat.model.LocalChatRoom;
import chat.model.RemoteChatRoom;
import chat.service.*;
import chat.improvements.FastBullyElection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatServer {

    @Option(name = "-n", usage = "n=Server ID")
    private String serverId = "1";


    private String serverConfig = "./config/server.tab";

    private File systemPropertiesFile = new File("./config/system.properties");

    private boolean debug = false;

    private boolean trace = false;

    private ServerState serverState = ServerState.getInstance();
    private ServerInfo serverInfo;
    private ExecutorService servicePool;
    private String mainHall;

    private Configuration systemProperties;
    private static final String ALIVE_JOB = "AliveJob".toUpperCase();
    private static final String ALIVE_JOB_TRIGGER = "AliveJobTrigger".toUpperCase();

    public ChatServer(String[] args) {
        try {
            CmdLineParser cmdLineParser = new CmdLineParser(this);

            cmdLineParser.parseArgument(args);

            System.out.println("ServerId --> "+ serverId );

            readServerConfiguration();

            try {
                Configurations configs = new Configurations();
                systemProperties = configs.properties(systemPropertiesFile);
            } catch (ConfigurationException e) {
                logger.error("Configuration error :  " + e.getLocalizedMessage());
            }



            serverState.initServerState(serverId);

            serverInfo = serverState.getServerInfo();

            updateLogger();

            serverState.setIsFastBully(systemProperties.getBoolean("election.fast.bully"));
            // T2
            serverState.setElectionAnswerTimeout(systemProperties.getLong("election.answer.timeout"));
            // T3
            serverState.setElectionCoordinatorTimeout(systemProperties.getLong("election.coordinator.timeout"));
            // T4
            serverState.setElectionNominationTimeout(systemProperties.getLong("election.nomination.timeout"));

            serverState.setupConnectedServers();


            mainHall = "MainHall-" + serverInfo.getServerId();
            LocalChatRoom localChatRoomInfo = new LocalChatRoom();
            localChatRoomInfo.setOwner(""); //The owner of the MainHall in each server is "" (empty string)
            localChatRoomInfo.setChatRoomId(mainHall);
            serverState.getLocalChatRooms().put(mainHall, localChatRoomInfo);

            startUpConnections();

            syncChatRooms();

            initiateCoordinator();

            System.out.println("Heartbeat starts running");
                startHeartBeat();

            Runtime.getRuntime().addShutdownHook(new ShutdownService(servicePool));

        } catch (CmdLineException e) {
            logger.error(e.getMessage());
        }
    }

    private void initiateCoordinator() {
        System.out.println("Starting initial coordinator election...");
        if (serverState.getServerInfoList().size() == 1) {

            serverState.setCoordinator(serverInfo);
        } else {

                new FastBullyElection().sendIamUpMessage(serverState.getServerInfo(),
                        serverState.getServerInfoList());
                try {

                    new FastBullyElection().startWaitingForViewMessage(serverState.getElectionAnswerTimeout());
                } catch (SchedulerException e) {
                    logger.error("Error while waiting for the view message at fast bully election: " +
                            e.getLocalizedMessage());
                }

        }
    }


    private void startHeartBeat() {
        try {

            JobDetail aliveJob = JobBuilder.newJob(Heartbeat.class)
                    .withIdentity(ALIVE_JOB, "group1").build();

            aliveJob.getJobDataMap().put("aliveErrorFactor", systemProperties.getInt("alive.error.factor"));

            Trigger aliveTrigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(ALIVE_JOB_TRIGGER, "group1")
                    .withSchedule(
                            SimpleScheduleBuilder.simpleSchedule()
                                    .withIntervalInSeconds(systemProperties.getInt("alive.interval")).repeatForever())
                    .build();

            Scheduler scheduler = Quartz.getInstance().getScheduler();
            scheduler.start();
            scheduler.scheduleJob(aliveJob, aliveTrigger);

        } catch (SchedulerException e) {
            logger.error(e.getLocalizedMessage());
        }
    }

    private void readServerConfiguration() {
        ColumnPositionMappingStrategy<ServerInfo> strategy = new ColumnPositionMappingStrategy<>();
        strategy.setType(ServerInfo.class);
        CsvToBean<ServerInfo> csvToBean = new CsvToBean<>();
        try {
            serverState.setServerInfoList(csvToBean.parse(strategy, new CSVReader(new FileReader(serverConfig), '\t')));
        } catch (FileNotFoundException e) {
            logger.error("Can not load config file from location: " + serverConfig);
            logger.trace(e.getMessage());
        }
    }

    private void updateLogger() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        for (Map.Entry entry : config.getLoggers().entrySet()) {
            String name = (String) entry.getKey();
            if (name.startsWith("chat")) {
                LoggerConfig loggerConfig = (LoggerConfig) entry.getValue();
                if (debug && !trace) {
                    loggerConfig.setLevel(Level.DEBUG);
                    ctx.updateLoggers();
                }

                if (trace) {
                    loggerConfig.setLevel(Level.TRACE);
                    ctx.updateLoggers();
                }
            }
        }

        if (debug && !trace) {
            System.out.println("Server is running in DEBUG mode");
        } else {
            logger.trace("Server is running in TRACE mode");
        }
    }

    private void startUpConnections() {
        servicePool = Executors.newFixedThreadPool(SERVER_SOCKET_POOL);
        try {
            servicePool.execute(new ClientService(serverInfo.getPort(), CLIENT_SOCKET_POOL));
            servicePool.execute(new ManagementService(serverInfo.getManagementPort(), serverState.getServerInfoList().size()));
        } catch (IOException e) {
            logger.trace(e.getMessage());
            servicePool.shutdown();
        }
    }

    private void syncChatRooms() {
        ServerCommunication peerClient = new ServerCommunication();
        JSONMessageBuilder messageBuilder = JSONMessageBuilder.getInstance();
        JSONParser parser = new JSONParser();

        for (ServerInfo server : serverState.getServerInfoList()) {
            if (server.equals(this.serverInfo)) continue;

            if (serverState.isOnline(server)) {
                // promote my main hall
                peerClient.commPeer(server, messageBuilder.serverUpMessage());
                peerClient.commPeer(server, messageBuilder.lockRoom(this.mainHall));
                peerClient.commPeer(server, messageBuilder.releaseRoom(this.mainHall, "true"));
                //TODO serverUpMessage to send even earlier?
                //String[] messages = {messageBuilder.serverUpMessage(), messageBuilder.lockRoom(this.mainHall), messageBuilder.releaseRoom(this.mainHall, "true")};
                //peerClient.commPeer(server, messages);

                // accept theirs
                String resp = peerClient.commServerSingleResp(server, messageBuilder.listRoomsClient());
                if (resp != null) {
                    try {
                        JSONObject jsonMessage = (JSONObject) parser.parse(resp);
                        logger.trace("syncChatRooms: " + jsonMessage.toJSONString());
                        JSONArray ja = (JSONArray) jsonMessage.get(Protocol.rooms.toString());
                        for (Object o : ja.toArray()) {
                            String room = (String) o;
                            if (serverState.isRoomExistedRemotely(room)) continue;
                            RemoteChatRoom remoteRoom = new RemoteChatRoom();
                            remoteRoom.setChatRoomId(room);
                            String serverId = server.getServerId();
                            if (room.startsWith("MainHall")) { // every server has MainHall-s* duplicated
                                String sid = room.split("-")[1];
                                if (!sid.equalsIgnoreCase(serverId)) {
                                    //serverId = sid; // Or skip
                                    continue;
                                }
                            }
                            remoteRoom.setManagingServer(serverId);
                            serverState.getRemoteChatRooms().put(room, remoteRoom);
                        }
                    } catch (ParseException e) {
                        //e.printStackTrace();
                        logger.trace(e.getMessage());
                    }
                }
            }
        }
    }



    private static final int SERVER_SOCKET_POOL = 2;
    private static final int CLIENT_SOCKET_POOL = 100;
    private static final Logger logger = LogManager.getLogger(ChatServer.class);
}
