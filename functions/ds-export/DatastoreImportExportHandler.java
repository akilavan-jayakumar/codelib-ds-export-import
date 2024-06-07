import com.catalyst.Context;
import com.catalyst.basic.BasicIO;
import com.catalyst.basic.ZCFunction;
import com.zc.common.ZCProject;
import enums.CommonResponseMessage;
import enums.JobOperation;
import handlers.DatastoreExportHandler;
import handlers.DatastoreImportHandler;
import pojos.JobDetail;
import services.DatastoreImportExportService;
import web.ResponseWrapper;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DatastoreImportExportHandler implements ZCFunction {
    private static final Logger LOGGER = Logger.getLogger(DatastoreImportExportHandler.class.getName());

    @Override
    public void runner(Context context, BasicIO basicIO) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper();
        try {

            ZCProject.initProject();

            String jobId = String.valueOf(basicIO.getParameter("jobId"));
            String domain = String.valueOf(basicIO.getParameter("domain"));

            if (jobId == null || jobId.isBlank()) {
                throw new Exception("jobId cannot be empty");
            }

            if (domain == null || domain.isBlank()) {
                throw new Exception("domain cannot be empty");
            }


            JobDetail jobDetail = DatastoreImportExportService.getJobDetailWithParamsById(jobId);

            if (jobDetail == null) {
                throw new Exception("Unable to retrieve the job detail for id " + jobId);
            }

            if (jobDetail.getOperation().equals(JobOperation.EXPORT.value)) {
                DatastoreExportHandler.handle(jobDetail, domain);
            } else {
                DatastoreImportHandler.handle(jobDetail, domain);
            }

            responseWrapper.setStatus(200);
            responseWrapper.setMessage(CommonResponseMessage.JOB_SUCCEEDED.message);

        } catch (Exception e) {
            responseWrapper.setStatus(500);
            responseWrapper.setMessage(CommonResponseMessage.JOB_FAILED.message);
            LOGGER.log(Level.SEVERE, "Exception in DatastoreExport", e);

        }

        basicIO.setStatus(responseWrapper.getStatus());
        basicIO.write(responseWrapper.getMessage());

    }

}