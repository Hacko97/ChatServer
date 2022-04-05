package chat.service;

import java.io.Serializable;
import java.util.Comparator;


public class ServerPriority implements Comparator<String>, Serializable {

    @Override
    public int compare(String server1, String server2) {
        if (null != server1 && null != server2) {
            Integer server1Id = Integer.parseInt(server1);
            Integer server2Id = Integer.parseInt(server2);
            return server2Id - server1Id;
        }
        return 0;
    }
}
