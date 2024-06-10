package services;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DiskFileService {

    private static final String BASE_DIR = "datastore-import-export";

    public static Path getTempDirectory() {
        return Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR + File.separator + Thread.currentThread().getId());
    }


    public static File createFile(String fileName) throws Exception {
        Path dirPath = getTempDirectory();
        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }

        return filePath.toFile();
    }

    public static File writeFile(String fileName, String content) throws Exception {
        File file = createFile(fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
        bufferedWriter.write(content);
        bufferedWriter.close();
        return file;
    }

    public static String readFile(File file) throws Exception {
        BufferedReader bufferedWriter = new BufferedReader(new FileReader(file));
        String content = bufferedWriter.lines().collect(Collectors.joining());
        bufferedWriter.close();

        return content;

    }

    public static void deleteFile(Path path) throws Exception {
        Files.delete(path);
    }

    public static void deleteDirectory(File file) throws Exception {
        if (file.exists()) {
            if (file.isDirectory()) {
                String[] children = file.list();
                if (children != null) {
                    for (String child : children) {
                        deleteDirectory(new File(file, child));
                    }
                }
            }
            DiskFileService.deleteFile(file.toPath());
        }
    }

    public static void flushBaseDirectory() throws Exception {
        deleteDirectory(getTempDirectory().toFile());
    }
}
