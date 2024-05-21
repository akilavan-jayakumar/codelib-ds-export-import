package pojos;

import java.io.File;
import java.util.Map;

public class InsertedRecordsDetails {
    private final File records;
    private final Map<String, String> mappings;

    public InsertedRecordsDetails(File records, Map<String, String> mappings) {
        this.records = records;
        this.mappings = mappings;
    }

    public File getRecords() {
        return records;
    }

    public Map<String, String> getMappings() {
        return mappings;
    }
}
