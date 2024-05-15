package utils;

import services.DiskFileService;

import java.io.File;

public class DiskFileUtil {
    public static File createFile(String folder, String fileName) throws Exception {
        return DiskFileService.createFile(folder, fileName);
    }
}
