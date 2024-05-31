package services;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class DiskFileService {

    private static final String BASE_DIR = "ds-export-import";


    public static File createFile(String folder, String fileName) throws Exception {
        Path dirPath;

        if (!folder.isBlank()) {
            dirPath = Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR + File.separator + folder);
        } else {
            dirPath = Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR);
        }


        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        if(Files.exists(filePath)){
            Files.delete(filePath);
        }

        return filePath.toFile();
    }

    public static File createFile(String fileName) throws Exception {
        return createFile("", fileName);
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
}
