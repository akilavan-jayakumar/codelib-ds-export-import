package restcontrollers;

import enums.*;
import exceptions.HttpException;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import pojos.JobDetail;
import pojos.JobDetailParam;
import pojos.SubJob;
import services.CatalystDatastoreService;
import services.CatalystJobService;
import services.DatastoreImportExportService;
import utils.CommonUtil;
import utils.DatastoreImportExportUtil;
import validations.DatastoreImportExportValidation;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;



public class DatastoreImportExportController {


    public static ResponseWrapper getJobDetail(HttpServletRequest httpServletRequest) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);

        String jobId = CommonUtil.parseRequestUri(httpServletRequest.getRequestURI()).get(1);

        JobDetail jobDetail = DatastoreImportExportService.getJobDetailWithParamsById(jobId);

        if (jobDetail == null) {
            throw new HttpException(CommonResponse.JOB_NOT_FOUND);
        }

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setData(jobDetail.getResponseMap());

        return responseWrapper;
    }

    public static ResponseWrapper getJobAsset(HttpServletRequest httpServletRequest) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_ZIP);

        String jobId = CommonUtil.parseRequestUri(httpServletRequest.getRequestURI()).get(1);

        JobDetail jobDetail = DatastoreImportExportService.getJobDetailById(jobId);

        if (jobDetail == null) {
            throw new HttpException(CommonResponse.JOB_NOT_FOUND);
        }

        if (jobDetail.getOperation().equals(JobOperation.EXPORT.value) && !jobDetail.getStatus().equals(JobStatus.SUCCESS.value)) {
            if (jobDetail.getStatus().equals(JobStatus.PENDING.value)) {
                throw new HttpException(CommonResponse.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_PENDING_JOB);
            } else if (jobDetail.getStatus().equals(JobStatus.RUNNING.value)) {
                throw new HttpException(CommonResponse.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_RUNNING_JOB);
            } else {
                throw new HttpException(CommonResponse.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_FAILED_JOB);
            }
        }


        File jobAsset = DatastoreImportExportService.downloadAsset(jobDetail.getAssetFileId(), jobDetail.getAssetFileName());

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setData(jobAsset);

        return responseWrapper;
    }

    public static ResponseWrapper createDatastoreExport(HttpServletRequest httpServletRequest) throws Exception {
        DatastoreImportExportValidation.preCheckJobCreation();

        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);
        String domain = CommonUtil.getDomainFromRequest(httpServletRequest);

        JobDetailParam jobDetailParam = new JobDetailParam();

        File paramsFile = DatastoreImportExportService.createParamsFile(jobDetailParam);
        String paramsFileId = DatastoreImportExportService.uploadAsset(paramsFile, true);

        JobDetail jobDetail = new JobDetail();
        jobDetail.setMessage("");
        jobDetail.setAssetFileId("");
        jobDetail.setAssetFileName("");
        jobDetail.setParams(jobDetailParam);
        jobDetail.setParamsFileId(paramsFileId);
        jobDetail.setStatus(JobStatus.PENDING.value);
        jobDetail.setParamsFileName(paramsFile.getName());
        jobDetail.setOperation(JobOperation.EXPORT.value);


        DatastoreImportExportService.createJobDetail(jobDetail);
        CatalystJobService.runImportExportJob(jobDetail.getRowId(), domain);

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setData(jobDetail.getResponseMap());

        return responseWrapper;
    }

    public static ResponseWrapper createDatastoreImport(HttpServletRequest httpServletRequest) throws Exception {
        DatastoreImportExportValidation.preCheckJobCreation();

        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);
        String domain = CommonUtil.getDomainFromRequest(httpServletRequest);

        Map<String, Object> requestBody = CommonUtil.parseMultipartFormData(httpServletRequest);

        if (!requestBody.containsKey("file") || requestBody.get("file") == null) {
            throw new HttpException(HttpStatus.BAD_REQUEST, "file cannot be empty");
        }

        Object fileObject = requestBody.get("file");

        if (!(fileObject instanceof File file) || !CommonUtil.getFileMimeType((File) fileObject).equals(MimeType.APPLICATION_ZIP.value)) {
            throw new HttpException(HttpStatus.BAD_REQUEST, "Invalid value for file. file should be a zip.");
        }

        String assetFileId = DatastoreImportExportService.uploadAsset(file, true);
        JobDetailParam jobDetailParam = new JobDetailParam();

        File paramsFile = DatastoreImportExportService.createParamsFile(jobDetailParam);
        String paramsFileId = DatastoreImportExportService.uploadAsset(paramsFile, true);

        JobDetail jobDetail = new JobDetail();
        jobDetail.setMessage("");
        jobDetail.setAssetFileId(assetFileId);
        jobDetail.setAssetFileName(file.getName());
        jobDetail.setParams(jobDetailParam);
        jobDetail.setParamsFileId(paramsFileId);
        jobDetail.setStatus(JobStatus.PENDING.value);
        jobDetail.setParamsFileName(paramsFile.getName());
        jobDetail.setOperation(JobOperation.IMPORT.value);


        DatastoreImportExportService.createJobDetail(jobDetail);
        CatalystJobService.runImportExportJob(jobDetail.getRowId(), domain);

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setData(jobDetail.getResponseMap());

        return responseWrapper;
    }

    public static ResponseWrapper onJobCallback(HttpServletRequest httpServletRequest) throws Exception {

        boolean  triggerCatalystImportExportJob = true;
        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);


        String domain = CommonUtil.getDomainFromRequest(httpServletRequest);
        String jobId = CommonUtil.parseRequestUri(httpServletRequest.getRequestURI()).get(1);
        JSONObject jsonRequestBody = CommonUtil.parseJsonRequestBody(httpServletRequest);

        String bulkJobId = jsonRequestBody.get("job_id").toString();
        String bulkStatus = jsonRequestBody.get("status").toString();
        JSONObject bulkResults = jsonRequestBody.getJSONObject("results");
        String bulkResultDescription = bulkResults.get("description").toString();

        JobDetail jobDetail = DatastoreImportExportService.getJobDetailWithParamsById(jobId);

        if (jobDetail == null) {
            throw new HttpException(CommonResponse.JOB_NOT_FOUND);
        }

        List<SubJob> subJobs = jobDetail.getParams().getSubJobs();
        Map<String,String> files = jobDetail.getParams().getFiles();
        SubJob currentSubJob = DatastoreImportExportUtil.getSubJobByBulkJobId(subJobs, bulkJobId);
        SubJob pendingSubJob = DatastoreImportExportUtil.getPendingSubJob(subJobs);

        if (!bulkStatus.equalsIgnoreCase("completed")) {
            jobDetail.setStatus(JobStatus.FAILURE.value);
            jobDetail.setMessage(bulkResultDescription);
            currentSubJob.setStatus(JobStatus.FAILURE.value);

            if(jobDetail.getStatus().equals(JobOperation.EXPORT.value)){
                currentSubJob.setFile("");
            }


            DatastoreImportExportService.persistJobDetail(jobDetail);

        } else if (jobDetail.getOperation().equals(JobOperation.EXPORT.value)) {
            boolean hasMoreRecords = bulkResults.getJSONArray("details").getJSONObject(0).getBoolean("more_records");
            String bulkReadResultFileName = DatastoreImportExportUtil.getCsvFileName(currentSubJob.getTable(), currentSubJob.getPage());

            File file = CatalystDatastoreService.downloadBulkReadReport(bulkJobId, bulkReadResultFileName);
            String fileId = DatastoreImportExportService.uploadAsset(file, true);

            currentSubJob.setFile(bulkReadResultFileName);
            currentSubJob.setStatus(JobStatus.SUCCESS.value);

            files.put(bulkReadResultFileName, fileId);

            if (hasMoreRecords) {
                SubJob subJob = new SubJob();
                subJob.setId(UUID.randomUUID().toString());
                subJob.setTable(currentSubJob.getTable());
                subJob.setPage(currentSubJob.getPage() + 1);
                subJob.setStatus(JobStatus.PENDING.value);
                subJob.setOperation(SubJobOperation.READ.value);
                subJob.setColumns(currentSubJob.getColumns());
                subJobs.add(subJob);
            }



            if (pendingSubJob != null) {
                triggerCatalystImportExportJob = false;
                String nextBulkReadJobId = CatalystDatastoreService.createBulkRead(pendingSubJob.getTable(), pendingSubJob.getPage(), DatastoreImportExportUtil.getJobCallbackUrl(domain, jobId), new HashMap<>());
                pendingSubJob.setStatus(JobStatus.RUNNING.value);
                pendingSubJob.setBulkJobId(nextBulkReadJobId);
            }

            DatastoreImportExportService.persistJobDetail(jobDetail);



        }else  {
            currentSubJob.setStatus(JobStatus.SUCCESS.value);

            if (pendingSubJob != null) {
                triggerCatalystImportExportJob = false;
                String nextBulkReadJobId = CatalystDatastoreService.createBulkWrite(pendingSubJob.getTable(),files.get(pendingSubJob.getFile()), DatastoreImportExportUtil.getJobCallbackUrl(domain, jobId), new HashMap<>());
                pendingSubJob.setStatus(JobStatus.RUNNING.value);
                pendingSubJob.setBulkJobId(nextBulkReadJobId);
            }

            DatastoreImportExportService.persistJobDetail(jobDetail);

        }


        if(triggerCatalystImportExportJob){
            CatalystJobService.runImportExportJob(jobId,domain);
        }

        responseWrapper.setHttpStatus(HttpStatus.OK);
        responseWrapper.setMessage(CommonResponseMessage.COMMON_PROCESSING_MESSAGE.message);

        return responseWrapper;

    }

}
