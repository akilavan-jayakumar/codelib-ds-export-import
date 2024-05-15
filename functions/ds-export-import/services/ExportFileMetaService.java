package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;
import pojos.ExportFileMeta;

import java.io.File;
import java.util.List;

public class ExportFileMetaService {


    private static void replaceFileMetas(List<ExportFileMeta> exportFileMetas) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES, objectMapper.writeValueAsString(exportFileMetas));
    }

    public static List<ExportFileMeta> getAllFileMetas() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        String jobs = catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES);

        return objectMapper.readValue(jobs, new TypeReference<List<ExportFileMeta>>() {
        });
    }

    public static void createFileMetas(List<ExportFileMeta> exportFileMetas) throws Exception {
        List<ExportFileMeta> allFileMetas = getAllFileMetas();
        allFileMetas.addAll(exportFileMetas);

        replaceFileMetas(allFileMetas);
    }

    public static String uploadFile(File file) throws Exception {
        CatalystFolderService catalystFolderService = new CatalystFolderService(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT);
        return catalystFolderService.uploadFile(file).getFileId().toString();
    }

}
