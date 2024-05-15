package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;
import pojos.ImportFileMeta;

import java.io.File;
import java.util.List;

public class ImportFileMetaService {

    private static void replaceImportFileMetas(List<ImportFileMeta> importFileMetas) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES, objectMapper.writeValueAsString(importFileMetas));
    }


    public static String uploadFile(File file) throws Exception {
        return CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).uploadFile(file);
    }

    public static List<ImportFileMeta> getAllImportFileMetas() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME);

        return objectMapper.readValue(catalystCacheService.getValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES, "[]"), new TypeReference<List<ImportFileMeta>>() {
        });
    }

    public static void createImportFileMetas(List<ImportFileMeta> importFileMetas) throws Exception {
        List<ImportFileMeta> allImportFileMetas = getAllImportFileMetas();
        allImportFileMetas.addAll(importFileMetas);
        replaceImportFileMetas(allImportFileMetas);
    }

    public static void deleteImportFileMetaAsset(String assetId) throws Exception {
        CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).deleteFile(assetId);
    }
}
