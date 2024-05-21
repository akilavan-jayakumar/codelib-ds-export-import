package utils;

import services.DiskFileService;

import java.io.File;
import java.util.List;

public class DiskFileUtil {
    private static final String CONTENTS_DIR = "contents";

    public static File createFile(String folder, String fileName) throws Exception {
        return DiskFileService.createFile(folder, fileName);
    }

    public static List<File> unzip(File zipFile, boolean deleteZipAfterUnzip) throws Exception {
        return DiskFileService.unzip(zipFile, CONTENTS_DIR, deleteZipAfterUnzip);
    }
}
