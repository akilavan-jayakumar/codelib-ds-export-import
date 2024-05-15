package services;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DiskFileService {

    private static final String BASE_DIR = "ds-export-import";


    public static File createFile(String folder, String fileName) throws Exception {
        Path dirPath = Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR + File.separator + folder);
        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        return filePath.toFile();
    }
}
