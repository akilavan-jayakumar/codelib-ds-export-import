package utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zc.component.files.ZCFileDetail;
import com.zc.component.object.bulk.result.ZCBulkResult;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;
import constants.DatastoreExportImportConstants;
import constants.DatastoreExportImportUrlConstants;
import enums.ExportAction;
import pojos.FileMeta;
import pojos.Job;
import services.CatalystCacheService;
import services.CatalystFolderService;
import services.CatalystJobService;
import services.CatalystTableService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class DatastoreExportUtil {
    public static boolean isDatastoreExporting() throws Exception {
        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);

        return catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT) != null;
    }

    public static void startExport(HashMap<String, String> params) throws Exception {
        params.put("action", ExportAction.START.value);
        CatalystJobService.createExportJob(params);
    }

    public static void endExport() throws Exception {
        HashMap<String, String> params = new HashMap<>();
        params.put("action", ExportAction.END.value);

        CatalystJobService.endExportJob(params);
    }

    public static List<Job> getAllJobs() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        String jobs = catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS);

        return objectMapper.readValue(jobs, new TypeReference<List<Job>>() {
        });
    }

    public static List<FileMeta> getAllFileMetas() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        String jobs = catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES);

        return objectMapper.readValue(jobs, new TypeReference<List<FileMeta>>() {
        });
    }

    public static void updateJobs(List<Job> jobs) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS, objectMapper.writeValueAsString(jobs));

    }

    public static void createTableExport(Job job, String domain) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(job.getTable());
        ZCBulkResult zcBulkResult = catalystTableService.createBulkExport(job.getPage(), domain + DatastoreExportImportUrlConstants.SERVER + DatastoreExportImportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreExportImportUrlConstants.EXPORT + DatastoreExportImportUrlConstants.CALLBACK, new HashMap<>());
        job.setId(zcBulkResult.getJobId().toString());
    }

    public static void createFileMeta(FileMeta fileMeta) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        List<FileMeta> fileMetas = getAllFileMetas();
        fileMetas.add(fileMeta);

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES, objectMapper.writeValueAsString(fileMetas));
    }

    public static FileMeta cloneJobAssetToFilestore(Job job) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(job.getTable());
        try (InputStream inputStream = catalystTableService.downloadBulkExport(job.getId())) {
            String tmpDir = System.getProperty("java.io.tmpdir") + File.separator + DatastoreExportImportConstants.BASE_DIR + File.separator + DatastoreExportImportConstants.CSV_FILES_DIR;
            Path tmpPath = Paths.get(tmpDir);

            if (!Files.exists(tmpPath)) {
                Files.createDirectories(tmpPath);
            }
            File file = new File(tmpDir + File.separator + job.getTable() + "-" + job.getPage());
            try (OutputStream outputStream = new FileOutputStream(file)) {
                int bytesRead;
                byte[] buffer = new byte[1024];
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
            FileMeta fileMeta = new FileMeta();
            CatalystFolderService catalystFolderService = new CatalystFolderService(CatalystFilestoreFolders.DS_IMPORT_EXPORT);
            ZCFileDetail zcFileDetail = catalystFolderService.uploadFile(file);

            fileMeta.setId(zcFileDetail.getFileId().toString());
            fileMeta.setName(zcFileDetail.getFileName());
            fileMeta.setTable(job.getTable());

            return fileMeta;
        }
    }


}
