package chat.model;

import org.apache.commons.lang3.StringUtils;

public class Utilities {

    private static final int MIN_CHAR = 2;
    private static final int MAX_CHAR = 17;

    public static boolean isIdValid(String id) {
        int length = id.length();
        return (StringUtils.isAlphanumeric(id) && length > MIN_CHAR && length < MAX_CHAR);
    }
}
