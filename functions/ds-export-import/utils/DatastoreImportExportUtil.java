package utils;

import constants.DatastoreExportImportUrlConstants;
import enums.JobStatus;
import pojos.SubJob;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DatastoreImportExportUtil {

    public static String getJobCallbackUrl(String domain, String jobId) {
        return domain + DatastoreExportImportUrlConstants.SERVER + DatastoreExportImportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreExportImportUrlConstants.JOBS + "/" + jobId + DatastoreExportImportUrlConstants.CALLBACK;
    }

    public static SubJob getSubJobByBulkJobId(List<SubJob> subJobs, String bulkJobId) {
        return subJobs.stream().filter(obj -> obj.getBulkJobId().equals(bulkJobId)).findAny().orElse(null);
    }

    public static SubJob getPendingSubJob(List<SubJob> subJobs) {
        return subJobs.stream().filter(obj -> obj.getStatus().equals(JobStatus.PENDING.value)).findAny().orElse(null);
    }

    public static String getCsvFileName(String tableName, Integer page) {
        StringJoiner stringJoiner = new StringJoiner("-");
        Arrays.stream(tableName.split(" ")).forEach(stringJoiner::add);
        stringJoiner.add(page.toString());
        return stringJoiner + ".csv";
    }


}
