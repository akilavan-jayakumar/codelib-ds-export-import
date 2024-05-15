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
import pojos.ExportFileMeta;
import pojos.ExportJob;
import utils.DatastoreExportUtil;
import utils.ExportFileMetaUtil;
import utils.ExportJobUtil;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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

        ExportJob currentExportJob = ExportJobUtil.getJobByJobId(zcBulkResult.getJobId().toString());

        if (currentExportJob == null) {
            throw new HttpException(CommonResponse.EXPORT_JOB_NOT_FOUND);
        }

        if (zcBulkResult.getStatus().equals(STATUS.FAILED)) {
            currentExportJob.setStatus(JobStatus.FAILURE.value);
            ExportJobUtil.updateJob(currentExportJob);
        } else {
            currentExportJob.setStatus(JobStatus.SUCCESS.value);
            ExportJobUtil.updateJob(currentExportJob);

            List<File> exportJobReports = ExportJobUtil.getExportJobReports(currentExportJob);
            List<ExportFileMeta> exportFileMetas = new ArrayList<>();

            for (File exportJobReport : exportJobReports) {
                ExportFileMeta exportFileMeta = new ExportFileMeta();
                exportFileMeta.setTable(currentExportJob.getTable());
                exportFileMeta.setName(exportJobReport.getName());
                exportFileMeta.setFile_id(ExportFileMetaUtil.uploadFile(exportJobReport));
                exportFileMetas.add(exportFileMeta);
            }


            ExportFileMetaUtil.createExportFileMetas(exportFileMetas);

            if (zcBulkResult.getResults().getDetails().get(0).getMoreRecords()) {
                ExportJob exportJob = new ExportJob();
                exportJob.setTable(currentExportJob.getTable());
                exportJob.setPage(currentExportJob.getPage() + 1);
                exportJob.setStatus(JobStatus.PENDING.value);
                ExportJobUtil.createJob(exportJob);
            }

            List<ExportJob> pendingExportJobs = ExportJobUtil.getAllJobs(Collections.singletonList(JobStatus.PENDING));

            if (!pendingExportJobs.isEmpty()) {
                ExportJob pendingExportJob = pendingExportJobs.get(0);
                ExportJobUtil.executeJob(pendingExportJob, getDomainFromRequest(httpServletRequest));

            } else {
                DatastoreExportUtil.endExport();
            }
        }

        responseWrapper.setHttpStatus(HttpStatus.OK);
        return responseWrapper;
    }
}
