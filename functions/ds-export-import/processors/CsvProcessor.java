package processors;

import utils.DiskFileUtil;

import java.io.*;
import java.util.logging.Logger;

public class CsvProcessor {

    private static final Logger LOGGER = Logger.getLogger(CsvProcessor.class.getName());
    private final File file;

    public CsvProcessor(File file) {
        this.file = file;
    }

    public int getTotalRecords() throws Exception {
        int count = -1;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        while (bufferedReader.readLine() != null) {
            count++;
        }
        return count;
    }

    public File slice(int start, int end, String folder, String fileName) throws Exception {
        int offset = 0;
        File chunkFile = DiskFileUtil.createFile(folder, fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String header = bufferedReader.readLine();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(chunkFile))) {
            String line;
            bufferedWriter.write(header + System.lineSeparator());
            while ((line = bufferedReader.readLine()) != null) {
                if (offset >= start && offset < end) {
                    bufferedWriter.write(line + System.lineSeparator());
                } else if (offset >= end) {
                    break;
                }
                offset++;
            }
        }

        return chunkFile;
    }

}
