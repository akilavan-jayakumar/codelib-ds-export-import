package services;

import java.io.*;
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

    public static File writeFile(String folder, String fileName, String content) throws Exception {
        File file = createFile(folder, fileName);
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
            bufferedWriter.write(content);
        }
        return file;
    }

    public static void writeFile(File file, String content) throws Exception {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
            bufferedWriter.write(content);
        }
    }

    public static String readFileContent(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()));
    }

    public static List<File> unzip(File zipFile, String folder, boolean deleteZipAfterUnzip) throws Exception {
        List<File> unzippedFiles = new ArrayList<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                File out = createFile(folder, zipEntry.getName());
                try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(out))) {
                    zipInputStream.transferTo(outputStream);
                }
                unzippedFiles.add(out);
            }
        }

        if (deleteZipAfterUnzip) {
            Files.delete(zipFile.toPath());
        }
        return unzippedFiles;
    }


}
