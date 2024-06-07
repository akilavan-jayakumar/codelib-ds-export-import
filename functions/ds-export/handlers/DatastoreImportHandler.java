package handlers;

import com.zc.component.object.ZCColumn;
import constants.CatalystTableConstants;
import constants.DatastoreImportExportConstants;
import enums.ColumnType;
import enums.CommonResponseMessage;
import enums.JobStatus;
import enums.SubJobOperation;
import pojos.*;
import processors.CsvProcessor;
import services.*;
import utils.DatastoreImportExportUtil;

import java.io.File;
import java.util.*;

public class DatastoreImportHandler {
    private static final String FILES_DIR = "files";

    private static List<Map<String, String>> getInsertSpecificRecords(Table table, List<Map<String, String>> records) {
        List<Map<String, String>> results = new ArrayList<>();

        List<String> keys = table.getColumns().stream().filter(obj -> !obj.getType().equals(ColumnType.FOREIGN_KEY.value) && !CatalystTableConstants.SystemColumns.ALL.contains(obj.getName())).map(Column::getName).toList();

        for (Map<String, String> oldRecord : records) {
            Map<String, String> newRecord = new HashMap<>();

            for (String key : keys) {
                newRecord.put(key, oldRecord.get(key));
            }

            results.add(newRecord);
        }

        return results;

    }

    private static List<Map<String, String>> getUpdateSpecificRecords(Table table, List<Map<String, String>> records, List<String> columns, Map<String, Map<String, String>> columnAndOldToNewIdMapping) {
        List<Map<String, String>> results = new ArrayList<>();

        List<String> keys = table.getColumns().stream().filter(obj -> (obj.getType().equals(ColumnType.FOREIGN_KEY.value) && columns.contains(obj.getName())) || (obj.getName().equals(CatalystTableConstants.SystemColumns.ROWID))).map(Column::getName).toList();


        for (Map<String, String> oldRecord : records) {
            Map<String, String> newRecord = new HashMap<>();

            for (String key : keys) {
                if (columnAndOldToNewIdMapping.containsKey(key)) {
                    newRecord.put(key, columnAndOldToNewIdMapping.get(key).get(oldRecord.get(key)));
                } else {
                    newRecord.put(key, oldRecord.get(key));
                }
            }

            results.add(newRecord);
        }

        return results;

    }

    private static List<SubJob> createAdditionalSubJobForDependentTable(Table table, String fileName) {
        List<SubJob> subJobs = new ArrayList<>();
        List<Column> foreignKeyColumns = DatastoreImportExportUtil.getForeignKeyColumns(table.getColumns());

        int totalForeignKeyColumns = foreignKeyColumns.size();
        int totalSubJobs = (int) Math.ceil((double) totalForeignKeyColumns / DatastoreImportExportConstants.DEPENDENT_TABLE_MAX_COLUMNS_PER_UPDATE);

        for (int i = 1; i <= totalSubJobs; i++) {
            int start = (i - 1) * DatastoreImportExportConstants.DEPENDENT_TABLE_MAX_COLUMNS_PER_UPDATE;
            int end = Math.min(start + DatastoreImportExportConstants.DEPENDENT_TABLE_MAX_COLUMNS_PER_UPDATE, totalForeignKeyColumns);

            SubJob subJob = new SubJob();
            subJob.setId(UUID.randomUUID().toString());
            subJob.setFile(fileName);
            subJob.setTable(table.getName());
            subJob.setStatus(JobStatus.PENDING.value);
            subJob.setOperation(SubJobOperation.UPDATE.value);
            subJob.setColumns(foreignKeyColumns.subList(start, end).stream().map(Column::getName).toList());

            subJobs.add(subJob);
        }

        return subJobs;
    }

    private static List<File> splitCsvIntoChunks(File file, String tableName, int pageNo, int maxRecords) throws Exception {

        List<File> files = new ArrayList<>();

        CsvProcessor csvProcessor = new CsvProcessor(file);
        int totalRecords = csvProcessor.getTotalRecords();

        if (totalRecords > maxRecords) {
            int totalChunks = (int) Math.ceil((double) totalRecords / maxRecords);

            for (int chunkNo = 1; chunkNo <= totalChunks; chunkNo++) {
                int start = (chunkNo - 1) * DatastoreImportExportConstants.DEPENDENT_TABLE_CSV_MAX_RECORDS;
                int end = Math.min(start + DatastoreImportExportConstants.DEPENDENT_TABLE_CSV_MAX_RECORDS, totalRecords);

                if (chunkNo == 1) {
                    start = chunkNo;
                }
                String fileName = DatastoreImportExportUtil.getCsvChunkFileName(tableName, pageNo, chunkNo);
                File chunkFile = csvProcessor.sliceAsFile(start, end, fileName);
                files.add(chunkFile);
            }

            DiskFileService.deleteFile(file);
        } else {
            files.add(file);
        }

        return files;

    }

    public static void handle(JobDetail jobDetail, String domain) throws Exception {
        boolean triggerCatalystImportExportJob = true;
        try {
            JobDetailParam jobDetailParam = jobDetail.getParams();
            List<Table> tables = jobDetailParam.getTables();
            List<SubJob> subJobs = jobDetailParam.getSubJobs();
            HashMap<String, String> files = (HashMap<String, String>) jobDetailParam.getFiles();

            if (jobDetail.getStatus().equals(JobStatus.PENDING.value)) {
                jobDetail.setStatus(JobStatus.RUNNING.value);

                File sourceZip = DatastoreImportExportService.downloadAsset(jobDetail.getAssetFileId(), jobDetail.getAssetFileName());
                List<File> zipFiles = DiskFileService.unzip(sourceZip, true);

                File tableMetaJsonFile = zipFiles.stream().filter(obj -> obj.getName().equals(DatastoreImportExportConstants.TABLE_META_JSON)).findAny().orElse(null);

                if (tableMetaJsonFile == null) {
                    throw new Exception(CommonResponseMessage.FILE_NOT_FOUND_IN_ZIP.message(DatastoreImportExportConstants.TABLE_META_JSON));
                }

                tables.addAll(DatastoreImportExportService.generateTablesFromTableMetaJsonFile(tableMetaJsonFile));

                List<ZCTableDetail> zcTableDetails = CatalystDatastoreService.getAllTablesInZCTableDetail();

                for (Table table : tables) {
                    ZCTableDetail zcTableDetail = zcTableDetails.stream().filter(obj -> obj.getZcTable().getName().equals(table.getName())).findAny().orElse(null);

                    if (zcTableDetail == null) {
                        throw new Exception(CommonResponseMessage.TABLE_NOT_FOUND.message(table.getName()));
                    }

                    List<ZCColumn> zcColumns = zcTableDetail.getZcColumns();

                    for (ZCColumn zcColumn : zcColumns) {
                        if (zcColumn.getIsMandatory()) {
                            throw new Exception(CommonResponseMessage.IMPORT_OPERATION_FORBIDDEN_DUE_TO_MANDATORY_COLUMN.message(zcColumn.getColumnName(), table.getName()));
                        }
                    }

                    for (Column column : table.getColumns()) {
                        ZCColumn zcColumn = zcColumns.stream().filter(obj -> obj.getColumnName().equals(column.getName())).findAny().orElse(null);

                        if (zcColumn == null) {
                            throw new Exception(CommonResponseMessage.COLUMN_NOT_FOUND.message(column.getName(), table.getName()));
                        }

                        if (!column.getType().equals(zcColumn.getDataType())) {
                            throw new Exception(CommonResponseMessage.DATATYPE_MISMATCH_FOR_COLUMN.message(column.getName(), table.getName(), column.getType(), zcColumn.getDataType()));
                        }

                        if (column.getType().equals(ColumnType.FOREIGN_KEY.value)) {
                            ZCTableDetail mappedTableForZCColumn = zcTableDetails.stream().filter(obj -> obj.getZcTable().getTableId().toString().equals(zcColumn.getParentTable())).findAny().orElse(null);

                            if (mappedTableForZCColumn != null && !(column.getParent().getTable().equals(mappedTableForZCColumn.getZcTable().getName()))) {
                                throw new Exception(CommonResponseMessage.PARENT_TABLE_CONFIGURATION_MISMATCH_FOR_FOREIGN_KEY_COLUMN.message(column.getName(), table.getName(), column.getParent().getTable(), mappedTableForZCColumn.getZcTable().getName()));
                            }
                        }
                    }

                }
                for (Table table : tables) {
                    boolean isParentTable = DatastoreImportExportUtil.isParentTable(tables, table.getName());
                    boolean isTableContainsForeignKey = DatastoreImportExportUtil.isTableContainsForeignKey(table);

                    int pageNo = 1;
                    for (String fileName : table.getFiles()) {
                        List<File> csvChunks = new ArrayList<>();
                        File csvFile = zipFiles.stream().filter(obj -> obj.getName().equals(fileName)).findAny().orElse(null);

                        if (csvFile == null) {
                            throw new Exception(CommonResponseMessage.FILE_NOT_FOUND_IN_ZIP.message(fileName));
                        }

                        if (!isParentTable && !isTableContainsForeignKey) {
                            csvChunks.addAll(splitCsvIntoChunks(csvFile, table.getName(), pageNo, DatastoreImportExportConstants.INDEPENDENT_TABLE_CSV_MAX_RECORDS));
                        } else {
                            csvChunks.addAll(splitCsvIntoChunks(csvFile, table.getName(), pageNo, DatastoreImportExportConstants.DEPENDENT_TABLE_CSV_MAX_RECORDS));
                        }


                        for (File csvChunk : csvChunks) {
                            String csvChunkFileName = csvChunk.getName();
                            String csvChunkFileId = DatastoreImportExportService.uploadAsset(csvChunk, true);

                            files.put(csvChunkFileName, csvChunkFileId);

                            SubJob subJob = new SubJob();
                            subJob.setId(UUID.randomUUID().toString());
                            subJob.setFile(csvChunkFileName);
                            subJob.setTable(table.getName());
                            subJob.setStatus(JobStatus.PENDING.value);
                            subJob.setColumns(Collections.emptyList());

                            if (!isParentTable && !isTableContainsForeignKey) {
                                subJob.setOperation(SubJobOperation.BULK_CREATION.value);
                            } else {
                                subJob.setOperation(SubJobOperation.CREATE.value);
                            }

                            subJobs.add(subJob);

                            if (isTableContainsForeignKey) {
                                subJobs.addAll(createAdditionalSubJobForDependentTable(table, csvChunkFileName));
                            }

                            Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);
                        }
                        pageNo++;
                    }
                }
            } else if (jobDetail.getStatus().equals(JobStatus.RUNNING.value)) {
                SubJob pendingSubJob = DatastoreImportExportUtil.getPendingSubJob(subJobs);
                if (pendingSubJob != null) {
                    Table table = DatastoreImportExportUtil.getTableByName(tables, pendingSubJob.getTable());

                    boolean isParentTable = DatastoreImportExportUtil.isParentTable(tables, table.getName());
                    boolean isTableContainsForeignKey = DatastoreImportExportUtil.isTableContainsForeignKey(table);

                    if (pendingSubJob.getOperation().equals(SubJobOperation.CREATE.value)) {
                        HashMap<String, String> oldToNewIdMapping = new HashMap<>();
                        CatalystTableService catalystTableService = new CatalystTableService(table.getName());

                        String csvFileName = pendingSubJob.getFile();
                        String oldCsvFileId = files.get(csvFileName);
                        String mappingFileName = DatastoreImportExportUtil.getJsonMappingFileName(pendingSubJob.getTable());
                        File oldCsvFile = DatastoreImportExportService.downloadAsset(oldCsvFileId, csvFileName);
                        CsvProcessor oldFileCsvProcessor = new CsvProcessor(oldCsvFile);

                        if (isParentTable && files.containsKey(mappingFileName)) {
                            oldToNewIdMapping.putAll(DatastoreImportExportService.getOldToNewIdMappings(files.get(mappingFileName), mappingFileName));
                        }


                        int totalRecords = oldFileCsvProcessor.getTotalRecords();
                        int totalOperations = (int) Math.ceil((double) totalRecords / 200);

                        List<String> header = oldFileCsvProcessor.getHeaders();
                        File newCsvFile = DiskFileService.createFile(FILES_DIR, csvFileName);
                        CsvProcessor newFileCsvProcessor = new CsvProcessor(newCsvFile);

                        if (isTableContainsForeignKey) {
                            newFileCsvProcessor.writeHeader(header);
                        }

                        for (int operationNo = 1; operationNo <= totalOperations; operationNo++) {
                            int start = (operationNo - 1) * 200;
                            int end = Math.min(start + 200, totalRecords);

                            if (operationNo == 1) {
                                start = operationNo;
                            }

                            List<Map<String, String>> actualRecords = oldFileCsvProcessor.sliceAsMap(start, end);
                            List<Map<String, String>> insertSpecificRecords = getInsertSpecificRecords(table, actualRecords);
                            catalystTableService.insertRecords(insertSpecificRecords);

                            if (isParentTable) {
                                for (int i = 0; i < actualRecords.size(); i++) {
                                    oldToNewIdMapping.put(actualRecords.get(i).get(CatalystTableConstants.SystemColumns.ROWID), insertSpecificRecords.get(i).get(CatalystTableConstants.SystemColumns.ROWID));
                                }
                            }

                            if (isTableContainsForeignKey) {
                                for (int i = 0; i < actualRecords.size(); i++) {
                                    actualRecords.get(i).put(CatalystTableConstants.SystemColumns.ROWID, insertSpecificRecords.get(i).get(CatalystTableConstants.SystemColumns.ROWID));
                                }

                                newFileCsvProcessor.writeRecords(actualRecords.stream().map(actualRecord -> {
                                    List<String> record = new ArrayList<>();
                                    for (String key : header) {
                                        record.add(actualRecord.get(key));
                                    }
                                    return record;
                                }).toList());
                            }

                            Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);

                        }

                        if (isParentTable) {
                            String oldToNewIdMappingFileId = DatastoreImportExportService.persistOldToNewIdMapping(oldToNewIdMapping, mappingFileName);

                            if (files.containsKey(mappingFileName)) {
                                DatastoreImportExportService.deleteAsset(files.get(mappingFileName));
                            }

                            files.put(mappingFileName, oldToNewIdMappingFileId);
                        }

                        DiskFileService.deleteFile(oldCsvFile);
                        DatastoreImportExportService.deleteAsset(oldCsvFileId);
                        files.remove(csvFileName);

                        if (isTableContainsForeignKey) {
                            String newFileId = DatastoreImportExportService.uploadAsset(newCsvFile, true);
                            files.put(csvFileName, newFileId);
                        }

                        pendingSubJob.setStatus(JobStatus.SUCCESS.value);
                    } else if (pendingSubJob.getOperation().equals(SubJobOperation.UPDATE.value)) {
                        Map<String, Map<String, String>> columnAndOldToNewIdMapping = new HashMap<>();
                        CatalystTableService catalystTableService = new CatalystTableService(table.getName());


                        for (Column column : table.getColumns()) {
                            if (column.getType().equals(ColumnType.FOREIGN_KEY.value) && pendingSubJob.getColumns().contains(column.getName())) {
                                String mappingFileName = DatastoreImportExportUtil.getJsonMappingFileName(column.getParent().getTable());
                                String mappingFileId = files.get(mappingFileName);
                                Map<String, String> oldToNewIdMapping = DatastoreImportExportService.getOldToNewIdMappings(mappingFileId, mappingFileName);

                                columnAndOldToNewIdMapping.put(column.getName(), oldToNewIdMapping);
                                Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);
                            }
                        }

                        String csvFileName = pendingSubJob.getFile();
                        String csvFileId = files.get(csvFileName);
                        File csvFile = DatastoreImportExportService.downloadAsset(csvFileId, csvFileName);

                        CsvProcessor csvProcessor = new CsvProcessor(csvFile);

                        int totalRecords = csvProcessor.getTotalRecords();
                        int totalOperations = (int) Math.ceil((double) totalRecords / 200);

                        for (int operationNo = 1; operationNo <= totalOperations; operationNo++) {
                            int start = (operationNo - 1) * 200;
                            int end = Math.min(start + 200, totalRecords);

                            if (operationNo == 1) {
                                start = operationNo;
                            }

                            List<Map<String, String>> actualRecords = csvProcessor.sliceAsMap(start, end);
                            List<Map<String, String>> updateSpecificRecords = getUpdateSpecificRecords(table, actualRecords, pendingSubJob.getColumns(), columnAndOldToNewIdMapping);
                            catalystTableService.updateRecords(updateSpecificRecords);
                        }

                        DiskFileService.deleteFile(csvFile);
                        DatastoreImportExportService.deleteAsset(csvFileId);
                        files.remove(csvFileName);

                        pendingSubJob.setStatus(JobStatus.SUCCESS.value);
                    } else {
                        String nextBulkReadJobId = CatalystDatastoreService.createBulkWrite(pendingSubJob.getTable(), files.get(pendingSubJob.getFile()), DatastoreImportExportUtil.getJobCallbackUrl(domain, jobDetail.getId()), new HashMap<>());
                        pendingSubJob.setStatus(JobStatus.RUNNING.value);
                        pendingSubJob.setBulkJobId(nextBulkReadJobId);
                    }
                } else {
                    triggerCatalystImportExportJob = false;

                    DatastoreImportExportService.deleteAssets(files.values().stream().toList());
                    files.clear();
                    subJobs.clear();

                    jobDetail.setStatus(JobStatus.SUCCESS.value);

                }

            }


        } catch (Exception exception) {
            jobDetail.setStatus(JobStatus.FAILURE.value);
            jobDetail.setMessage(exception.getMessage());

            throw exception;
        } finally {
            DatastoreImportExportService.persistJobDetail(jobDetail);
        }

        if (triggerCatalystImportExportJob) {
            CatalystJobService.runImportExportJob(jobDetail.getId(), domain);
        }
    }
}
