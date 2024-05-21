package handlers;

import enums.ImportJobOperation;
import enums.ImportJobStatus;
import pojos.Column;
import pojos.ImportJob;
import pojos.Table;

import java.util.HashMap;
import java.util.List;

public class ImportHandler {
    private final String currentJobId;
    private final List<Table> tables;
    private final List<ImportJob> importJobs;
    private final HashMap<String, String> files;

    public ImportHandler(List<Table> tables, HashMap<String, String> files, List<ImportJob> importJobs) {
        this.files = files;
        this.tables = tables;
        this.currentJobId = null;
        this.importJobs = importJobs;
    }

    public ImportHandler(List<Table> tables, HashMap<String, String> files, List<ImportJob> importJobs, String currentJobId) {
        this.files = files;
        this.tables = tables;
        this.importJobs = importJobs;
        this.currentJobId = currentJobId;
    }

    public List<ImportJob> getImportJobs() {
        return importJobs;
    }

    public ImportJob getCurrentImportJob() {
        if (this.currentJobId == null) {
            return null;
        }

        return this.importJobs.stream().filter(obj -> obj.getId().equals(currentJobId)).findAny().orElse(null);
    }

    public ImportJob getNextPendingJob() {
        List<ImportJob> insertPendingJobs = this.importJobs.stream().filter(obj -> obj.getStatus().equals(ImportJobStatus.PENDING.value) && obj.getOperation().equals(ImportJobOperation.INSERT.value)).toList();
        List<ImportJob> updatePendingJobs = this.importJobs.stream().filter(obj -> obj.getStatus().equals(ImportJobStatus.PENDING.value) && obj.getOperation().equals(ImportJobOperation.UPDATE.value)).toList();

        if (!insertPendingJobs.isEmpty()) {
            return insertPendingJobs.get(0);
        } else if (!updatePendingJobs.isEmpty()) {
            return updatePendingJobs.get(0);
        } else {
            return null;
        }

    }


    public boolean isPendingJobExits() {
        return !(this.importJobs.stream().filter(obj -> obj.getStatus().equals(ImportJobStatus.PENDING.value)).toList().isEmpty());
    }


    public void updateImportJob(ImportJob importJob) {
        int index = -1;
        for (int i = 0; i < this.importJobs.size(); i++) {
            if (this.importJobs.get(i).getId().equals(importJob.getId())) {
                index = i;
            }
        }

        if (index != -1) {
            this.importJobs.set(index, importJob);
        }
    }

    public List<String> getAllFileIds() {
        return this.files.values().stream().toList();
    }

    public String getFileId(String fileName) {
        return this.files.get(fileName);
    }

    public boolean containsFileId(String fileName) {
        return this.files.containsKey(fileName);
    }

    public void putFileId(String fileName, String fileId) {
        this.files.put(fileName, fileId);
    }

    public boolean isFileEntryExists(String key) {
        return this.files.containsKey(key);
    }

    public Table getTable(String tableName) {
        return this.tables.stream().filter(obj -> obj.getName().equals(tableName)).findAny().orElse(null);
    }

    public boolean isParentTable(String tableName) {
        for (Table table : this.tables) {
            for (Column column : table.getColumns()) {
                if (column.getParent() != null && column.getParent().getTable().equals(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public HashMap<String, String> getFiles() {
        return files;
    }
}
