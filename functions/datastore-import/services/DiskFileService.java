package services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DiskFileService {

    private static final String BASE_DIR = "datastore-export-import";

    private static String getTempDir() throws Exception {
        Path dirPath = Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        return dirPath.toString();
    }


    public static File createFile(String folder, String fileName) throws Exception {
        Path dirPath = Paths.get(getTempDir() + File.separator + folder);
        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        return filePath.toFile();
    }


    public static String readFileContent(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()));
    }

    public static List<File> unzip(File zipFile, String folder, Boolean deleteSource) throws Exception {
        List<File> unzippedFiles = new ArrayList<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                File outputFile = createFile(folder, zipEntry.getName());
                try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
                    int bytesRead;
                    byte[] buffer = new byte[8 * 1024];
                    while ((bytesRead = zipInputStream.read(buffer)) > 0) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
                unzippedFiles.add(outputFile);
            }
        }

        if (deleteSource) {
            Files.delete(zipFile.toPath());
        }
        return unzippedFiles;
    }

    public static List<File> unzip(File zipFile, String folder) throws Exception {
        return unzip(zipFile, folder, true);
    }
}
