package services;

import com.zc.component.files.ZCFile;
import com.zc.component.files.ZCFileDetail;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

public class CatalystFolderService {
    private final String folder;

    public CatalystFolderService(String folder) {
        this.folder = folder;
    }

    public ZCFileDetail uploadFile(File file, boolean deleteSource) throws Exception {
        ZCFileDetail zcFileDetail = ZCFile.getInstance().getFolder(folder).uploadFile(file);
        
        if(deleteSource){
            Files.delete(file.toPath());
        }
        return zcFileDetail;
    }

    public ZCFileDetail uploadFile(File file) throws Exception {
        return uploadFile(file, true);
    }

    public InputStream downloadFile(String fileId) throws Exception {
        return ZCFile.getInstance().getFolder(folder).downloadFile(Long.parseLong(fileId));
    }
}
