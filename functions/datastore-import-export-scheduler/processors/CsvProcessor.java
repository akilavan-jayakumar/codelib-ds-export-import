package processors;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import services.DiskFileService;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class CsvProcessor {

    private final File file;

    public CsvProcessor(File file) {
        this.file = file;
    }

    public int getTotalRecords() throws Exception {
        int count = -1;
        try (CSVReader csvReader = new CSVReader(new FileReader(file))) {
            while (csvReader.readNext() != null) {
                count++;
            }
        }
        return count;
    }

    public List<String> getHeaders() throws Exception {

        try (CSVReader csvReader = new CSVReader(new FileReader(file))) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                return Arrays.stream(line).toList();
            }
        }

        return Collections.emptyList();

    }

    public File sliceAsFile(int start, int end, String fileName) throws Exception {
        int offset = 0;
        File chunkFile = DiskFileService.createFile(fileName);
        try (CSVReader csvReader = new CSVReader(new FileReader(file)); CSVWriter csvWriter = new CSVWriter(new FileWriter(chunkFile))) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                if (offset == 0) {
                    csvWriter.writeNext(line);
                } else if (offset >= start && offset <= end) {
                    csvWriter.writeNext(line);
                }

                if (offset >= end) {
                    break;
                }
                offset++;
            }

        }
        return chunkFile;
    }

    public List<Map<String, String>> sliceAsMap(int start, int end) throws Exception {
        int offset = 0;
        List<Map<String, String>> records = new ArrayList<>();

        try (CSVReader csvReader = new CSVReader(new FileReader(file))) {
            String[] line;
            String[] header = null;
            while ((line = csvReader.readNext()) != null) {
                if (offset == 0) {
                    header = line;
                } else if (offset >= start && offset <= end) {
                    Map<String, String> record = new HashMap<>();
                    for (int i = 0; i < header.length; i++) {
                        record.put(header[i], line[i]);
                    }
                    records.add(record);
                }
                if (offset >= end) {
                    break;
                }
                offset++;
            }
        }

        return records;
    }

    public void writeHeader(List<String> header) throws Exception {
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(file))) {
            csvWriter.writeNext(header.toArray(String[]::new));
        }
    }

    public void writeRecords(List<List<String>> records) throws Exception {
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(file, true))) {
            for (List<String> record : records) {
                csvWriter.writeNext(record.toArray(String[]::new));
            }
        }
    }

}
