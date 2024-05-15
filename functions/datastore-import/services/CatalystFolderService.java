package services;

import com.zc.component.files.ZCFile;
import com.zc.component.files.ZCFileDetail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;

public class CatalystFolderService {
    private final String folder;

    public CatalystFolderService(String folder) {
        this.folder = folder;
    }

    public static CatalystFolderService getInstance(String folder) {
        return new CatalystFolderService(folder);
    }

    public String uploadFile(File file, boolean deleteSource) throws Exception {
        ZCFileDetail zcFileDetail = ZCFile.getInstance().getFolder(folder).uploadFile(file);

        if (deleteSource) {
            Files.delete(file.toPath());
        }
        return zcFileDetail.getFileId().toString();
    }

    public String uploadFile(File file) throws Exception {
        return uploadFile(file, true);
    }

    public InputStream downloadFile(String fileId) throws Exception {
        return ZCFile.getInstance().getFolder(folder).downloadFile(Long.parseLong(fileId));
    }

    public File downloadFile(String fileId, String diskFolder, String diskFileName) throws Exception {
        File outputFile = DiskFileService.createFile(diskFolder, diskFileName);
        try (InputStream inputStream = downloadFile(fileId)) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
                int bytesRead;
                byte[] buffer = new byte[8 * 1024];
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
            }
        }

        return outputFile;

    }

    public void deleteFile(String fileId) throws Exception {
        ZCFile.getInstance().getFolder(folder).deleteFile(Long.parseLong(fileId));
    }
}
