package utils;

import constants.CatalystDatastoreTableConstants;
import enums.JobAction;
import pojos.Column;
import pojos.ImportFileMeta;
import pojos.ImportJob;
import pojos.Table;
import processors.CsvProcessor;
import services.DatastoreImportService;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class DatastoreImportUtil {

    private static final Long OPERATION_DELAY = 1000L;
    private static final Integer MAX_RECORDS_PER_OPERATION = 200;


    public static List<File> readFilesFromZip(String fileId) throws Exception {
        return DatastoreImportService.readFilesFromZip(fileId);
    }

    public static void runLoadImportJob(String jobId) throws Exception {
        HashMap<String, String> params = new HashMap<>();

        params.put("jobId", jobId);
        params.put("action", JobAction.LOAD.value);

        DatastoreImportService.createImportJob(params);
    }

    public static File downloadImportAsset(String fileId, String fileName) throws Exception {
        return DatastoreImportService.downloadImportAsset(fileId, fileName);
    }


    public static void rewriteImportJobsFiles(List<ImportJob> importJobs, List<ImportFileMeta> importFileMetas) throws Exception {
        for (ImportJob importJob : importJobs) {
            importFileMetas.stream().filter(obj -> obj.getName().equals(importJob.getFile_id())).findAny().ifPresent(importFileMeta -> importJob.setFile_id(importFileMeta.getFile_id()));
        }
    }

    public static void insertRecords(File file, Table table) throws Exception {
        CsvProcessor csvProcessor = new CsvProcessor(file);

        List<Column> foreignKeyColumns = table.getColumns().stream().filter(obj -> obj.getParent() != null).toList();

        int totalRecords = csvProcessor.getTotalRecords();
        int totalPages = (int) Math.ceil((double) totalRecords / MAX_RECORDS_PER_OPERATION);

        for (int i = 1; i <= totalPages; i++) {
            int start = (i - 1) * MAX_RECORDS_PER_OPERATION;
            int end = start + MAX_RECORDS_PER_OPERATION;

            List<HashMap<String, String>> records = csvProcessor.getRecordsAsJson(start, end);

            if (!foreignKeyColumns.isEmpty()) {
                for (Column column : foreignKeyColumns) {
                    for (HashMap<String, String> hashMap : records) {
                        hashMap.put(column.getName(), null);
                    }
                }
            }
            // Removing system fields
            records.forEach(record -> {
                record.remove(CatalystDatastoreTableConstants.SystemFields.ROWID);
                record.remove(CatalystDatastoreTableConstants.SystemFields.CREATORID);
                record.remove(CatalystDatastoreTableConstants.SystemFields.CREATEDTIME);
                record.remove(CatalystDatastoreTableConstants.SystemFields.MODIFIEDTIME);
            });

            DatastoreImportService.insertRecords(table.getName(), records);
            Thread.sleep(OPERATION_DELAY);
        }
    }

    public static void runEndImportJob() throws Exception {
        HashMap<String, String> params = new HashMap<>();

        params.put("action", JobAction.END.value);
        DatastoreImportService.createImportJob(params);
    }

    public static void clearImportJobsAssets() throws Exception {
       List<ImportFileMeta> importFileMetas = ImportFileMetaUtil.getAllImportFileMeta();

       for (ImportFileMeta importFileMeta:importFileMetas){
           ImportFileMetaUtil.deleteFileMetaAsset(importFileMeta.getFile_id());
           Thread.sleep(OPERATION_DELAY);
       }

       DatastoreImportService.clearImportOperation();
    }
}
