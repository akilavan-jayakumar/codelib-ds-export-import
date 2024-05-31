package utils;

import constants.CatalystDatastoreTableConstants;
import constants.DatastoreExportImportConstants;
import csv.CsvReader;
import csv.CsvWriter;
import enums.ImportJobOperation;
import enums.ImportJobStatus;
import enums.JobAction;
import handlers.ImportHandler;
import pojos.Column;
import pojos.ImportJob;
import pojos.InsertedRecordsDetails;
import pojos.Table;
import services.DatastoreImportService;

import java.io.File;
import java.util.*;

public class DatastoreImportUtil {

    private static final Integer MAX_COLUMNS_PER_UPDATE = 2;
    private static final Integer MAX_RECORDS_PER_OPERATION = 200;

    private static boolean isSystemColumn(String columnName) {
        return CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID.equals(columnName) || CatalystDatastoreTableConstants.SystemDefinedColumns.CREATORID.equals(columnName) || CatalystDatastoreTableConstants.SystemDefinedColumns.CREATEDTIME.equals(columnName) || CatalystDatastoreTableConstants.SystemDefinedColumns.MODIFIEDTIME.equals(columnName);
    }

    public static String getForeignKeyMappingJsonFileName(String tableName) {
        String processedTableName = tableName.replaceAll(" ", "-");
        return processedTableName + "-" + DatastoreExportImportConstants.FOREIGN_KEY_MAPPING_JSON_SUFFIX;
    }

    public static List<Table> getTablesFromFile(File file) throws Exception {
        return DatastoreImportService.getTablesFromFile(file);
    }

    public static ImportHandler getImportDetails(String currentJobId) throws Exception {
        HashMap<String, String> files = DatastoreImportService.getFilesInfo();

        File jobsFile = downloadImportAsset(files.get(DatastoreExportImportConstants.JOBS_JSON), DatastoreExportImportConstants.JOBS_JSON);
        File tablesFile = downloadImportAsset(files.get(DatastoreExportImportConstants.TABLE_META_JSON), DatastoreExportImportConstants.TABLE_META_JSON);

        List<Table> tables = DatastoreImportService.getTablesFromFile(tablesFile);
        List<ImportJob> importJobs = DatastoreImportService.getImportJobsFromFile(jobsFile);

        return new ImportHandler(tables, files, importJobs, currentJobId);
    }

    public static ImportHandler getImportDetails() throws Exception {
        return getImportDetails(null);
    }


    public static List<ImportJob> createImportJobsFromTables(List<Table> tables) throws Exception {
        List<ImportJob> importJobs = new ArrayList<>();

        for (Table table : tables) {

            List<Column> foreignKeyColumns = table.getColumns().stream().filter(obj -> obj.getParent() != null).toList();

            for (String file : table.getFiles()) {
                ImportJob importJob = new ImportJob();
                importJob.setFile(file);
                importJob.setTable(table.getName());
                importJob.setOperation(ImportJobOperation.INSERT.value);
                importJob.setStatus(ImportJobStatus.PENDING.value);
                importJob.setColumns(Collections.emptyList());
                importJobs.add(0, importJob);
            }


            if (!foreignKeyColumns.isEmpty()) {
                int totalColumns = foreignKeyColumns.size();
                for (int i = 0; i < totalColumns; i += MAX_COLUMNS_PER_UPDATE) {
                    List<String> columns = foreignKeyColumns.subList(i, Math.min(totalColumns, i + MAX_COLUMNS_PER_UPDATE)).stream().map(Column::getName).toList();

                    for (String file : table.getFiles()) {
                        ImportJob importJob = new ImportJob();
                        importJob.setFile(file);
                        importJob.setTable(table.getName());
                        importJob.setOperation(ImportJobOperation.UPDATE.value);
                        importJob.setStatus(ImportJobStatus.PENDING.value);
                        importJob.setColumns(columns);
                        importJobs.add(importJob);
                    }
                }
            }
        }

        return importJobs;
    }

    public static InsertedRecordsDetails insertRecords(File csv, Table table) throws Exception {
        String folder = String.valueOf(System.currentTimeMillis());

        File out = DiskFileUtil.createFile(folder, csv.getName());
        HashMap<String, String> mappings = new HashMap<>();


        CsvReader csvReader = new CsvReader(csv);
        CsvWriter csvWriter = new CsvWriter(out);


        List<String> columns = table.getColumns().stream().filter(obj -> obj.getParent() == null && !isSystemColumn(obj.getName())).map(Column::getName).toList();

        int totalRecords = csvReader.getTotalRecords();
        int totalPages = (int) Math.ceil((double) totalRecords / MAX_RECORDS_PER_OPERATION);

        for (int page = 1; page <= totalPages; page++) {
            List<String> oldIdsMapping = new ArrayList<>();
            List<HashMap<String, String>> tempRecords = new ArrayList<>();

            int start = (page - 1) * MAX_RECORDS_PER_OPERATION;
            int end = start + MAX_RECORDS_PER_OPERATION;

            List<HashMap<String, String>> oldRecords = csvReader.getRecordsAsJson(start, end);

            oldRecords.forEach(obj -> {
                oldIdsMapping.add(obj.get(CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID));
                HashMap<String, String> record = new HashMap<>();
                for (String column : columns) {
                    record.put(column, obj.get(column));
                }
                tempRecords.add(record);
            });


            List<HashMap<String, String>> newRecords = DatastoreImportService.insertRecords(table.getName(), tempRecords);

            for (int i = 0; i < newRecords.size(); i++) {
                String rowid = newRecords.get(i).get(CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID);

                mappings.put(oldIdsMapping.get(i), rowid);
                oldRecords.get(i).put(CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID, rowid);
            }

            csvWriter.writeRecords(oldRecords);
            Thread.sleep(DatastoreExportImportConstants.OPERATION_DELAY);
        }


        return new InsertedRecordsDetails(out, mappings);
    }

    public static void updateRecords(ImportHandler importHandler) throws Exception {
        Map<String, Map<String, String>> columnForeignKeyIdMappings = new HashMap<>();

        ImportJob currentImportJob = importHandler.getCurrentImportJob();
        Table table = importHandler.getTable(currentImportJob.getTable());


        for (String columnName : currentImportJob.getColumns()) {
            Column column = table.getColumns().stream().filter(obj -> obj.getName().equals(columnName)).findAny().orElse(null);

            if (column != null) {
                String foreignKeyIdMappingsFileName = getForeignKeyMappingJsonFileName(column.getParent().getTable());
                Map<String, String> foreignKeyIdMappings = DatastoreImportService.downloadForeignKeyIdMappings(importHandler.getFileId(foreignKeyIdMappingsFileName), foreignKeyIdMappingsFileName);
                columnForeignKeyIdMappings.put(columnName, foreignKeyIdMappings);
                Thread.sleep(DatastoreExportImportConstants.OPERATION_DELAY);
            }

        }

        File csv = downloadImportAsset(importHandler.getFileId(currentImportJob.getFile()), currentImportJob.getFile());
        CsvReader csvReader = new CsvReader(csv);

        int totalRecords = csvReader.getTotalRecords();
        int totalPages = (int) Math.ceil((double) totalRecords / MAX_RECORDS_PER_OPERATION);

        for (int i = 1; i <= totalPages; i++) {
            List<HashMap<String, String>> tempRecords = new ArrayList<>();

            int start = (i - 1) * MAX_RECORDS_PER_OPERATION;
            int end = start + MAX_RECORDS_PER_OPERATION;

            List<HashMap<String, String>> records = csvReader.getRecordsAsJson(start, end);

            records.forEach(obj -> {
                HashMap<String, String> record = new HashMap<>();
                record.put(CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID, obj.get(CatalystDatastoreTableConstants.SystemDefinedColumns.ROWID));
                for (String column : currentImportJob.getColumns()) {
                    record.put(column, columnForeignKeyIdMappings.get(column).get(obj.get(column)));
                }
                tempRecords.add(record);
            });

            DatastoreImportService.updateRecords(table.getName(), tempRecords);
            Thread.sleep(DatastoreExportImportConstants.OPERATION_DELAY);
        }


    }

    public static void persistImportDetails(ImportHandler importHandler) throws Exception {
        File jobFile = DatastoreImportService.createFileFromImportJobs(DatastoreExportImportConstants.JOBS_JSON, importHandler.getImportJobs());
        String jobFileId = uploadImportAsset(jobFile);

        if (importHandler.isFileEntryExists(DatastoreExportImportConstants.JOBS_JSON)) {
            deleteImportAsset(importHandler.getFileId(DatastoreExportImportConstants.JOBS_JSON));
        }

        importHandler.putFileId(DatastoreExportImportConstants.JOBS_JSON, jobFileId);
        DatastoreImportService.persistFilesInfo(importHandler.getFiles());

    }

    public static File createMappingFile(String fileName, Map<String, String> mapping) throws Exception {
        return DatastoreImportService.writeForeignKeyIdMappingsToFile(fileName, mapping);
    }

    public static void appendMappingToExistingFile(File file, Map<String, String> mappings) throws Exception {
        DatastoreImportService.appendForeignKeyIdMappingsToFile(file, mappings);
    }

    public static File downloadImportAsset(String fileId, String fileName) throws Exception {
        return DatastoreImportService.downloadImportAsset(fileId, fileName);
    }

    public static String uploadImportAsset(File file) throws Exception {
        return DatastoreImportService.uploadImportAsset(file);
    }

    public static void deleteImportAsset(String fileId) throws Exception {
        DatastoreImportService.deleteImportAsset(fileId);
    }

    public static void runLoadImportJob(String jobId) throws Exception {
        HashMap<String, String> params = new HashMap<>();

        params.put("jobId", jobId);
        params.put("action", JobAction.LOAD.value);

        DatastoreImportService.createImportJob(params);
    }

    public static void runEndImportJob() throws Exception {
        HashMap<String, String> params = new HashMap<>();

        params.put("action", JobAction.END.value);
        DatastoreImportService.createImportJob(params);
    }


    public static void cleanImportJobsAssets(ImportHandler importHandler) throws Exception {
        List<String> fileIds = importHandler.getAllFileIds();

        for (String fileId : fileIds) {
            deleteImportAsset(fileId);
            Thread.sleep(DatastoreExportImportConstants.OPERATION_DELAY);
        }
        DatastoreImportService.purgeFiles();
    }
}
