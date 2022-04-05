package chat.service;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

public class Quartz {

    private static Quartz instance;
    private static Scheduler scheduler;

    private Quartz() {
    }

    public static synchronized Quartz getInstance() {
        if (instance == null) {
            instance = new Quartz();
        }
        return instance;
    }

    public synchronized Scheduler getScheduler() {
        if (scheduler == null) {
            try {
                scheduler = StdSchedulerFactory.getDefaultScheduler();
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
        return scheduler;
    }
}
