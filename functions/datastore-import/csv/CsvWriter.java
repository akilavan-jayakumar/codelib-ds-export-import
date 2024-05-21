package csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class CsvWriter {
    private final File file;
    private boolean isHeaderStored;

    public CsvWriter(File file) {
        this.file = file;
        this.isHeaderStored = false;
    }

    private String convertToCsvContent(List<String> data) {
        return data.stream().map(this::escapeSpecialCharacters).collect(Collectors.joining(","));
    }

    private String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        if (data.contains(",") || data.contains("\"") || data.contains("'")) {
            data = data.replace("\"", "\"\"");
            escapedData = "\"" + data + "\"";
        }
        return escapedData;
    }

    public void writeRecords(List<HashMap<String, String>> records) throws Exception {
        String header = convertToCsvContent(records.get(0).keySet().stream().toList());
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, isHeaderStored))) {
            if (!isHeaderStored) {
                bufferedWriter.write(header + System.lineSeparator());
                this.isHeaderStored = true;
            }

            for (HashMap<String, String> record : records) {
                String content = convertToCsvContent(record.values().stream().toList());
                bufferedWriter.write(content + System.lineSeparator());
            }
        }
    }
}
