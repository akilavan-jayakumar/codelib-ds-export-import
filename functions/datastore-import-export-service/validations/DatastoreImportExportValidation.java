package validations;

import com.zc.component.object.ZCTable;
import enums.CommonResponse;
import enums.JobOperation;
import exceptions.HttpException;
import pojos.JobDetail;
import services.CatalystDatastoreService;
import services.DatastoreImportExportService;

import java.util.List;

public class DatastoreImportExportValidation {

    public static void preCheckJobCreation() throws Exception {

        JobDetail pendingOrRunningJobDetail = DatastoreImportExportService.getPendingOrRunningJobDetail();

        if (pendingOrRunningJobDetail != null) {
            if (pendingOrRunningJobDetail.getOperation().equals(JobOperation.IMPORT.value)) {
                throw new HttpException(CommonResponse.JOB_CREATION_FORBIDDEN_DUE_TO_IMPORT);
            } else {
                throw new HttpException(CommonResponse.JOB_CREATION_FORBIDDEN_DUE_TO_EXPORT);
            }
        }

        List<ZCTable> zcTables = CatalystDatastoreService.getAllTables();

        if (zcTables.isEmpty()) {
            throw new HttpException(CommonResponse.JOB_CREATION_FORBIDDEN_DUE_TO_NO_TABLE);
        }
    }
}
