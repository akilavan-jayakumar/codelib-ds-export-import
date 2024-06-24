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
import java.util.zip.ZipOutputStream;

public class DiskFileService {


    private static final String BASE_DIR = "datastore-import-export";

    private static Path getBaseDirectoryPath() {
        return Paths.get(System.getProperty("java.io.tmpdir") + File.separator + BASE_DIR + File.separator + Thread.currentThread().getId());
    }

    public static File createFile(String directory, String fileName) throws Exception {
        Path dirPath = Paths.get(getBaseDirectoryPath() + File.separator + directory);
        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }

        return filePath.toFile();
    }

    public static File createFile(String fileName) throws Exception {
        Path dirPath = getBaseDirectoryPath();
        Path filePath = Paths.get(dirPath + File.separator + fileName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }

        return filePath.toFile();
    }

    public static File createDirectory(String directoryName) throws Exception {
        Path dirPath = Paths.get(getBaseDirectoryPath() + File.separator + directoryName);

        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        return dirPath.toFile();
    }


    public static File createZip(List<File> files, String fileName, boolean deleteSourceFiles) throws Exception {
        File zipFile = createFile(fileName);
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile))) {
            for (File file : files) {
                Path path = file.toPath();
                ZipEntry zipEntry = new ZipEntry(file.getName());
                zipOutputStream.putNextEntry(zipEntry);
                Files.copy(path, zipOutputStream);
                zipOutputStream.closeEntry();

                if (deleteSourceFiles) {
                    Files.delete(path);
                }
            }
        }
        return zipFile;
    }

    public static List<File> unzip(File file, boolean deleteSource) throws Exception {
        List<File> files = new ArrayList<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(file))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                File entryFile;
                if (zipEntry.isDirectory()) {
                    entryFile = createDirectory(zipEntry.getName());
                } else {
                    entryFile = createFile(zipEntry.getName());
                }
                try (FileOutputStream fileOutputStream = new FileOutputStream(entryFile)) {
                    int bytesRead;
                    byte[] buffer = new byte[8 * 1024];
                    while ((bytesRead = zipInputStream.read(buffer)) > 0) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
                files.add(entryFile);

            }
        }
        return files;
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

    public static void deleteFile(File file) throws Exception {
        deleteFile(file.toPath());
    }
}
