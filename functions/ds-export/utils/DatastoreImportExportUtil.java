package utils;

import constants.DatastoreImportExportConstants;
import constants.DatastoreImportExportUrlConstants;
import enums.ColumnType;
import enums.JobStatus;
import enums.SubJobOperation;
import pojos.Column;
import pojos.SubJob;
import pojos.Table;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DatastoreImportExportUtil {

    public static String getExportZipFileName(String jobId) {
        StringJoiner stringJoiner = new StringJoiner("-");
        stringJoiner.add(DatastoreImportExportConstants.EXPORT_ZIP_FILE_PREFIX);
        stringJoiner.add(jobId);
        return stringJoiner + ".zip";
    }

    public static String getJobCallbackUrl(String domain, String jobId) {
        return domain + DatastoreImportExportUrlConstants.SERVER + DatastoreImportExportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreImportExportUrlConstants.JOBS + "/" + jobId + DatastoreImportExportUrlConstants.CALLBACK;
    }

    public static Table getTableByName(List<Table> tables, String tableName) {
        return tables.stream().filter(obj -> obj.getName().equals(tableName)).findAny().orElse(null);
    }

    public static List<Column> getForeignKeyColumns(List<Column> columns) {
        return columns.stream().filter(obj -> obj.getType().equals(ColumnType.FOREIGN_KEY.value)).toList();
    }

    public static boolean isParentTable(List<Table> tables, String tableName) {
        for (Table table : tables) {
            for (Column column : table.getColumns()) {
                if (column.getType().equals(ColumnType.FOREIGN_KEY.value) && column.getParent().getTable().equals(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isTableContainsForeignKey(Table table) {
        for (Column column : table.getColumns()) {
            if (column.getType().equals(ColumnType.FOREIGN_KEY.value)) {
                return true;
            }
        }

        return false;
    }

    public static String getCsvChunkFileName(String table, Integer page, Integer chunkNo) {
        StringJoiner stringJoiner = new StringJoiner("-");
        Arrays.stream(table.split(" ")).forEach(stringJoiner::add);
        stringJoiner.add(page.toString());
        stringJoiner.add(chunkNo.toString());
        return stringJoiner + ".csv";
    }

    public static String getJsonMappingFileName(String tableName) {
        StringJoiner stringJoiner = new StringJoiner("-");
        Arrays.stream(tableName.split(" ")).forEach(stringJoiner::add);
        stringJoiner.add(DatastoreImportExportConstants.MAPPING_JSON_SUFFIX);
        return stringJoiner.toString();
    }


    public static SubJob getPendingSubJob(List<SubJob> subJobs) {
        List<SubJob> pendingSubJobs = subJobs.stream().filter(obj -> obj.getStatus().equals(JobStatus.PENDING.value)).toList();
        List<SubJob> createPendingSubJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.CREATE.value)).toList();
        List<SubJob> updatePendingSubJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.UPDATE.value)).toList();
        List<SubJob> bulkCreatePendingJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.BULK_CREATION.value)).toList();


        if (pendingSubJobs.isEmpty()) {
            return null;
        } else if (!createPendingSubJobs.isEmpty()) {
            return createPendingSubJobs.get(0);
        } else if (!updatePendingSubJobs.isEmpty()) {
            return updatePendingSubJobs.get(0);
        } else {
            return bulkCreatePendingJobs.get(0);
        }


    }

}
