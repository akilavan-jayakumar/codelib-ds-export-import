package utils;

import pojos.ImportFileMeta;
import services.ImportFileMetaService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ImportFileMetaUtil {
    private static final Long FILE_UPLOAD_DELAY = 1000L;

    public static List<ImportFileMeta> getAllImportFileMeta() throws Exception {
        return ImportFileMetaService.getAllImportFileMetas();
    }

    public static ImportFileMeta getImportFileMetaByFileId(String fileId) throws Exception {
        return getAllImportFileMeta().stream().filter(obj -> obj.getFile_id().equals(fileId)).findAny().orElse(null);
    }

    public static List<ImportFileMeta> convertFilesToImportFileMetas(List<File> files) throws Exception {
        List<ImportFileMeta> importFileMetas = new ArrayList<>();

        for (File file : files) {
            ImportFileMeta importFileMeta = new ImportFileMeta();
            String uploadedFileId = ImportFileMetaService.uploadFile(file);
            importFileMeta.setFile_id(uploadedFileId);
            importFileMeta.setName(file.getName());
            importFileMetas.add(importFileMeta);
            Thread.sleep(FILE_UPLOAD_DELAY);
        }

        return importFileMetas;
    }

    public static void createFileMetas(List<ImportFileMeta> importFileMetas) throws Exception {
        ImportFileMetaService.createImportFileMetas(importFileMetas);
    }

    public static void deleteFileMetaAsset(String fileId) throws Exception {
        ImportFileMetaService.deleteImportFileMetaAsset(fileId);
    }
}
