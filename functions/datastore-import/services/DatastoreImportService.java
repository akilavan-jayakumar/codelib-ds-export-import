package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zc.component.object.ZCRowObject;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;
import pojos.ImportJob;
import pojos.Table;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatastoreImportService {
    private static final String CONTENT_DIR = "files";

    private static List<ZCRowObject> convertHashMapToZCRowObject(List<HashMap<String, String>> records) {
        return records.stream().map(obj -> {
            ZCRowObject zcRowObject = ZCRowObject.getInstance();
            obj.forEach(zcRowObject::set);
            return zcRowObject;
        }).toList();
    }


    public static HashMap<String, String> getFilesInfo() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String value = CatalystCacheService.getInstance(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME).getValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES);
        return objectMapper.readValue(value, new TypeReference<HashMap<String, String>>() {
        });
    }

    public static List<Table> getTablesFromFile(File file) throws Exception {
        String content = DiskFileService.readFileContent(file);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(content, new TypeReference<List<Table>>() {
        });
    }

    public static List<ImportJob> getImportJobsFromFile(File file) throws Exception {
        String content = DiskFileService.readFileContent(file);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(content, new TypeReference<List<ImportJob>>() {
        });
    }

    public static void persistFilesInfo(HashMap<String, String> files) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CatalystCacheService.getInstance(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME).putValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES, objectMapper.writeValueAsString(files));
    }


    public static List<HashMap<String, String>> insertRecords(String table, List<HashMap<String, String>> records) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(table);
        return catalystTableService.insertRecords(convertHashMapToZCRowObject(records));
    }

    public static void updateRecords(String table, List<HashMap<String, String>> records) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(table);
        catalystTableService.updateRecords(convertHashMapToZCRowObject(records));

    }

    public static void purgeFiles() throws Exception {
        CatalystCacheService.getInstance(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME).deleteValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES);
    }

    public static File createFileFromImportJobs(String fileName, List<ImportJob> importJobs) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return DiskFileService.writeFile(CONTENT_DIR, fileName, objectMapper.writeValueAsString(importJobs));

    }

    public static File writeForeignKeyIdMappingsToFile(String fileName, Map<String, String> mappings) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return DiskFileService.writeFile(CONTENT_DIR, fileName, objectMapper.writeValueAsString(mappings));
    }

    public static void appendForeignKeyIdMappingsToFile(File file, Map<String, String> mappings) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String content = DiskFileService.readFileContent(file);

        Map<String, String> existingMappings = objectMapper.readValue(content, new TypeReference<Map<String, String>>() {
        });
        existingMappings.putAll(mappings);

        DiskFileService.writeFile(file, objectMapper.writeValueAsString(existingMappings));
    }


    public static String uploadImportAsset(File file) throws Exception {
        return CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).uploadFile(file);
    }

    public static File downloadImportAsset(String fileId, String fileName) throws Exception {
        return CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).downloadFile(fileId, CONTENT_DIR, fileName);
    }

    public static Map<String, String> downloadForeignKeyIdMappings(String fileId, String fileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).downloadFile(fileId, CONTENT_DIR, fileName);
        String content = DiskFileService.readFileContent(file);

        return objectMapper.readValue(content, new TypeReference<Map<String, String>>() {
        });
    }

    public static void deleteImportAsset(String fileId) throws Exception {
        CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).deleteFile(fileId);
    }

    public static void createImportJob(HashMap<String, String> params) throws Exception {
        CatalystJobService.createImportJob(params);
    }


}
