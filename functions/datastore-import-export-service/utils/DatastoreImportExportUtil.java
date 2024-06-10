package utils;

import constants.DatastoreExportImportUrlConstants;
import enums.JobStatus;
import enums.SubJobOperation;
import pojos.SubJob;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DatastoreImportExportUtil {

    public static String getJobCallbackUrl(String domain, String jobId) {
        return domain + DatastoreExportImportUrlConstants.SERVER + DatastoreExportImportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreExportImportUrlConstants.JOBS + "/" + jobId + DatastoreExportImportUrlConstants.CALLBACK;
    }

    public static SubJob getSubJobByBulkJobId(List<SubJob> subJobs, String bulkJobId) {
        return subJobs.stream().filter(obj -> obj.getBulkJobId() != null && obj.getBulkJobId().equals(bulkJobId)).findAny().orElse(null);
    }

    public static SubJob getPendingSubJob(List<SubJob> subJobs) {
        List<SubJob> pendingSubJobs = subJobs.stream().filter(obj -> obj.getStatus().equals(JobStatus.PENDING.value)).toList();
        List<SubJob> createPendingSubJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.CREATE.value)).toList();
        List<SubJob> updatePendingSubJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.UPDATE.value)).toList();
        List<SubJob> bulkCreatePendingJobs = pendingSubJobs.stream().filter(obj -> obj.getOperation().equals(SubJobOperation.BULK_CREATE.value)).toList();


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

    public static String getCsvFileName(String tableName, Integer page) {
        StringJoiner stringJoiner = new StringJoiner("-");
        Arrays.stream(tableName.split(" ")).forEach(stringJoiner::add);
        stringJoiner.add(page.toString());
        return stringJoiner + ".csv";
    }


}
