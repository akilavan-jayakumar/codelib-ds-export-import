package restcontrollers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zc.component.object.bulk.result.STATUS;
import com.zc.component.object.bulk.result.ZCBulkResult;
import enums.CommonResponse;
import enums.CommonResponseMessage;
import enums.JobStatus;
import exceptions.HttpException;
import org.springframework.http.HttpStatus;
import pojos.FileMeta;
import pojos.Job;
import utils.DatastoreExportUtil;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class DatastoreExportController {


    private static String getDomainFromRequest(HttpServletRequest httpServletRequest) {
        return "https://" + httpServletRequest.getHeader("host");
    }

    public static ResponseWrapper startExport(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {

        ResponseWrapper responseWrapper = new ResponseWrapper();

        if (DatastoreExportUtil.isDatastoreExporting()) {
            throw new HttpException(CommonResponse.DATASTORE_EXPORT_FORBIDDEN);
        }

        HashMap<String, String> params = new HashMap<>();
        params.put("domain", getDomainFromRequest(httpServletRequest));

        DatastoreExportUtil.startExport(params);

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setMessage(CommonResponseMessage.DATASTORE_EXPORT_SCHEDULED.message());

        return responseWrapper;
    }

    public static ResponseWrapper onExportCallback(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {

        ResponseWrapper responseWrapper = new ResponseWrapper();
        ObjectMapper objectMapper = new ObjectMapper();

        String requestBodyString = httpServletRequest.getReader().lines().collect(Collectors.joining());

        ZCBulkResult zcBulkResult = objectMapper.readValue(requestBodyString, new TypeReference<ZCBulkResult>() {
        });

        List<Job> jobs = DatastoreExportUtil.getAllJobs();
        Job job = jobs.stream().filter(obj -> obj.getId().equals(zcBulkResult.getJobId().toString())).findFirst().orElse(null);

        if (job == null) {
            throw new HttpException(CommonResponse.EXPORT_JOB_NOT_FOUND);
        }

        if (zcBulkResult.getStatus().equals(STATUS.FAILED)) {
            job.setStatus(JobStatus.FAILURE.value);
            DatastoreExportUtil.updateJobs(jobs);
        } else {
            job.setStatus(JobStatus.SUCCESS.value);
            FileMeta fileMeta = DatastoreExportUtil.cloneJobAssetToFilestore(job);
            DatastoreExportUtil.createFileMeta(fileMeta);

            List<Job> pendingJobs = jobs.stream().filter(obj -> obj.getStatus().equals(JobStatus.PENDING.value)).toList();

            if (!pendingJobs.isEmpty()) {
                Job pendingJob = pendingJobs.get(0);
                DatastoreExportUtil.createTableExport(pendingJob, getDomainFromRequest(httpServletRequest));
                DatastoreExportUtil.updateJobs(jobs);
            } else {
                DatastoreExportUtil.updateJobs(jobs);
                DatastoreExportUtil.endExport();
            }
        }

        responseWrapper.setHttpStatus(HttpStatus.OK);
        return responseWrapper;
    }
}
