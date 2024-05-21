package csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CsvReader {

    private static final String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private final File file;

    public CsvReader(File file) {
        this.file = file;
    }

    private String preProcessCsvRecord(String record) {

        if (record.startsWith("\"")) {
            record = record.substring(1);
        }

        if (record.endsWith("\"")) {
            record = record.substring(0, record.length() - 1);
        }

        return record;
    }

    public int getTotalRecords() throws Exception {
        int count = -1;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        while (bufferedReader.readLine() != null) {
            count++;
        }
        return count;
    }

    public List<HashMap<String, String>> getRecordsAsJson(int start, int end) throws Exception {
        String line;
        int offset = 0;
        List<HashMap<String, String>> records = new ArrayList<>();

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        List<String> headers = Arrays.stream(bufferedReader.readLine().split(COMMA_DELIMITER)).map(this::preProcessCsvRecord).toList();

        while ((line = bufferedReader.readLine()) != null) {
            if (offset >= start && offset < end) {
                HashMap<String, String> hashMap = new HashMap<>();
                List<String> values = Arrays.stream(line.split(COMMA_DELIMITER)).map(this::preProcessCsvRecord).toList();
                for (int i = 0; i < headers.size(); i++) {
                    hashMap.put(headers.get(i), values.get(i));
                }
                records.add(hashMap);
            } else if (offset >= end) {
                break;
            }
            offset++;
        }

        return records;

    }

}
