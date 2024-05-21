package utils;

import enums.ImportJobOperation;
import enums.ImportJobStatus;
import pojos.Column;
import pojos.ImportFileMeta;
import pojos.ImportJob;
import pojos.Table;
import services.ImportJobService;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class ImportJobUtil {

    private static final Integer MAX_COLUMNS_PER_UPDATE = 20;

    public static List<ImportJob> getAllImportJobs() throws Exception {
        return ImportJobService.getAllImportJobs();
    }

    public static List<ImportJob> getAllImportJobsByStatus(ImportJobStatus importJobStatus) throws Exception {
        return ImportJobService.getAllImportJobs().stream().filter(obj -> obj.getStatus().equals(importJobStatus.value)).toList();
    }

    public static ImportJob getImportJobById(String jobId) throws Exception {
        return ImportJobService.getAllImportJobs().stream().filter(obj -> obj.getId().equals(jobId)).findAny().orElse(null);
    }

    public static List<ImportJob> createImportJobsFromTable(List<Table> tables,List<File> files) throws Exception {
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
                    List<String> columns = foreignKeyColumns.subList(i, Math.min(totalColumns, i + MAX_COLUMNS_PER_UPDATE)).stream().map(column -> {
                        String source = new StringJoiner(".").add(table.getName()).add(column.getName()).toString();
                        String destination = new StringJoiner(".").add(column.getParent().getTable()).add(column.getParent().getColumn()).toString();
                        return new StringJoiner("-").add(source).add(destination).toString();
                    }).toList();

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

    public static File downloadJobAsset(String jobId) throws Exception {
        ImportJob importJob = getImportJobById(jobId);
        ImportFileMeta importFileMeta = ImportFileMetaUtil.getImportFileMetaByFileId(importJob.getFile());
        return DatastoreImportUtil.downloadImportAsset(importFileMeta.getFile_id(), importFileMeta.getName());
    }

    public static void createImportJobs(List<ImportJob> importJobs) throws Exception {
        ImportJobService.createImportJobs(importJobs);
    }


    public static void updateImportJob(ImportJob importJob) throws Exception {
        ImportJobService.updateImportJobs(Collections.singletonList(importJob));
    }


}
