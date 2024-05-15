package services;

import com.zc.component.object.ZCRowObject;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class DatastoreImportService {
    private static final String CONTENT_DIR = "files";
    private static final String IMPORT_ZIP_NAME = "import.zip";

    private static List<ZCRowObject> convertHashMapToZCRowObject(List<HashMap<String, String>> records) {
        return records.stream().map(obj -> {
            ZCRowObject zcRowObject = ZCRowObject.getInstance();
            obj.forEach(zcRowObject::set);
            return zcRowObject;
        }).toList();
    }

    public static List<File> readFilesFromZip(String fileId) throws Exception {
        File zipFile = CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).downloadFile(fileId, CONTENT_DIR, IMPORT_ZIP_NAME);
        return DiskFileService.unzip(zipFile, CONTENT_DIR);
    }


    public static void createImportJob(HashMap<String, String> params) throws Exception {
        CatalystJobService.createImportJob(params);
    }

    public static List<ZCRowObject> insertRecords(String table, List<HashMap<String, String>> records) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(table);
        return catalystTableService.insertRecords(convertHashMapToZCRowObject(records));

    }

    public static File downloadImportAsset(String fileId, String fileName) throws Exception {
        return CatalystFolderService.getInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).downloadFile(fileId, CONTENT_DIR, fileName);
    }

    public static void clearImportOperation() throws Exception{
        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME);
        catalystCacheService.deleteValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_JOBS);
        catalystCacheService.deleteValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_FILES);
    }
}
