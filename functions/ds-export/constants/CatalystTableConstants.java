package constants;

import java.util.List;

public class CatalystTableConstants {
    public static final class SystemColumns {
        public static final String ROWID = "ROWID";
        public static final String CREATORID = "CREATORID";
        public static final String CREATEDTIME = "CREATEDTIME";
        public static final String MODIFIEDTIME = "MODIFIEDTIME";
        public static final List<String> ALL = List.of(ROWID, CREATORID, CREATEDTIME, MODIFIEDTIME);
    }
}
