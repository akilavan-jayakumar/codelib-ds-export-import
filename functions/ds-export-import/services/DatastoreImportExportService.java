package services;


import com.catalystsolutions.querybuilder.builders.CriteriaBuilder;
import com.catalystsolutions.querybuilder.builders.OrderByBuilder;
import com.catalystsolutions.querybuilder.builders.QueryBuilder;
import com.catalystsolutions.querybuilder.enums.Operation;
import com.catalystsolutions.querybuilder.enums.SortOrder;
import com.catalystsolutions.querybuilder.pojos.OrderBy;
import com.catalystsolutions.querybuilder.utils.QueryUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zc.component.files.ZCFile;
import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowObject;
import com.zc.component.zcql.ZCQL;
import constants.CatalystFilestoreFolders;
import constants.DatastoreExportImportConstants;
import enums.JobStatus;
import pojos.JobDetail;
import pojos.JobDetailParam;
import tablefilters.DatastoreImportExportJobDetailsTableFilter;
import tables.DatastoreImportExportJobDetailsTable;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

public class DatastoreImportExportService {

    private static List<JobDetail> convertZCRowObjectsToJobDetails(List<ZCRowObject> zcRowObjects) {
        return zcRowObjects.stream().map(obj -> {
            JobDetail jobDetail = new JobDetail();
            jobDetail.loadFromQueryResult(obj);
            return jobDetail;
        }).toList();
    }

    private static DatastoreImportExportJobDetailsTableFilter createDatastoreExportImportJobDetailsTableFilter(List<String> rowIds, List<Integer> statuses, List<Integer> operations) {
        DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter = DatastoreImportExportJobDetailsTableFilter.getInstance();

        if (!rowIds.isEmpty()) {
            datastoreImportExportJobDetailsTableFilter.rowIds.addAll(rowIds.stream().map(QueryUtil::toQueryString).toList());
        }

        if (!statuses.isEmpty()) {
            datastoreImportExportJobDetailsTableFilter.statuses.addAll(statuses.stream().map(String::valueOf).map(QueryUtil::toQueryString).toList());
        }

        if (!operations.isEmpty()) {
            datastoreImportExportJobDetailsTableFilter.operations.addAll(operations.stream().map(String::valueOf).map(QueryUtil::toQueryString).toList());
        }


        return datastoreImportExportJobDetailsTableFilter;
    }

    private static List<JobDetail> getJobDetailsByLimit(Integer offset, Integer limit, DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter) throws Exception {
        OrderByBuilder orderByBuilder = OrderByBuilder.getInstance();
        orderByBuilder.add(OrderBy.getInstance(DatastoreImportExportJobDetailsTable.CREATEDTIME, SortOrder.DESC));

        CriteriaBuilder criteriaBuilder = CriteriaBuilder.getInstance();
        criteriaBuilder.add(datastoreImportExportJobDetailsTableFilter.rowIds);
        criteriaBuilder.add(datastoreImportExportJobDetailsTableFilter.statuses);
        criteriaBuilder.add(datastoreImportExportJobDetailsTableFilter.operations);

        QueryBuilder queryBuilder = QueryBuilder.getInstance(Operation.SELECT);
        queryBuilder.select(DatastoreImportExportJobDetailsTable.ROWID, DatastoreImportExportJobDetailsTable.STATUS, DatastoreImportExportJobDetailsTable.MESSAGE, DatastoreImportExportJobDetailsTable.OPERATION, DatastoreImportExportJobDetailsTable.CREATEDTIME, DatastoreImportExportJobDetailsTable.PARAMS_FILE_ID, DatastoreImportExportJobDetailsTable.ASSET_FILE_ID, DatastoreImportExportJobDetailsTable.PARAMS_FILE_NAME, DatastoreImportExportJobDetailsTable.ASSET_FILE_NAME);
        queryBuilder.from(DatastoreImportExportJobDetailsTable.NAME);
        queryBuilder.where(criteriaBuilder.build());
        queryBuilder.orderBy(orderByBuilder.build());
        queryBuilder.limit(limit);
        queryBuilder.offset(offset);


        List<ZCRowObject> zcRowObjects = ZCQL.getInstance().executeQuery(queryBuilder.build());
        return convertZCRowObjectsToJobDetails(zcRowObjects);

    }

    public static List<JobDetail> getJobDetailsByLimit(Integer offset, Integer limit, List<Integer> statuses, List<Integer> operations) throws Exception {
        DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(Collections.emptyList(), statuses, operations);
        return getJobDetailsByLimit(offset, limit, datastoreImportExportJobDetailsTableFilter);
    }

    public static JobDetail getJobDetailById(String jobId) throws Exception {
        DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(List.of(jobId), Collections.emptyList(), Collections.emptyList());
        return getJobDetailsByLimit(1, 1, datastoreImportExportJobDetailsTableFilter).stream().findAny().orElse(null);
    }

    public static JobDetail getJobDetailWithParamsById(String jobId) throws Exception {
        JobDetail jobDetail = getJobDetailById(jobId);

        if (jobDetail == null) {
            return null;
        }

        JobDetailParam jobDetailParam = getJobDetailParams(jobDetail.getParamsFileId(), jobDetail.getParamsFileName());
        jobDetail.setParams(jobDetailParam);


        return jobDetail;
    }

    public static JobDetail getPendingOrRunningJobDetail() throws Exception {
        DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(Collections.emptyList(), List.of(JobStatus.PENDING.value, JobStatus.RUNNING.value), Collections.emptyList());
        return getJobDetailsByLimit(1, 1, datastoreImportExportJobDetailsTableFilter).stream().findAny().orElse(null);
    }

    public static JobDetailParam getJobDetailParams(String paramsFileId, String paramsFileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = downloadAsset(paramsFileId, paramsFileName);
        String json = DiskFileService.readFile(file);

        JobDetailParam jobDetailParam = objectMapper.readValue(json, new TypeReference<JobDetailParam>() {
        });

        DiskFileService.deleteFile(file.toPath());

        return jobDetailParam;
    }

    public static void createJobDetails(List<JobDetail> jobDetails) throws Exception {
        List<ZCRowObject> rowObjects = ZCObject.getInstance().getTable(DatastoreImportExportJobDetailsTable.NAME).insertRows(jobDetails.stream().map(JobDetail::getInsertPayload).toList());

        for (int i = 0; i < rowObjects.size(); i++) {
            jobDetails.get(i).setRowId(rowObjects.get(i).get(DatastoreImportExportJobDetailsTable.ROWID.Raw.value).toString());
            jobDetails.get(i).setCreatedTime(rowObjects.get(i).get(DatastoreImportExportJobDetailsTable.CREATEDTIME.Raw.value).toString());
        }
    }

    public static void createJobDetail(JobDetail jobDetail) throws Exception {
        createJobDetails(List.of(jobDetail));
    }

    public static void updateJobDetails(List<JobDetail> jobDetails) throws Exception {
        ZCObject.getInstance().getTable(DatastoreImportExportJobDetailsTable.NAME).updateRows(jobDetails.stream().map(JobDetail::getUpdatePayload).toList());
    }

    public static void updateJobDetail(JobDetail jobDetail) throws Exception {
        updateJobDetails(List.of(jobDetail));
    }

    public static File createParamsFile(JobDetailParam jobDetailParam) throws Exception {
        StringJoiner stringJoiner = new StringJoiner("-");
        stringJoiner.add(UUID.randomUUID().toString());
        stringJoiner.add(DatastoreExportImportConstants.PARAMS_JSON);

        ObjectMapper objectMapper = new ObjectMapper();
        String content = objectMapper.writeValueAsString(jobDetailParam);

        return DiskFileService.writeFile(stringJoiner.toString(), content);
    }

    public static String uploadAsset(File file, boolean deleteSource) throws Exception {
        String fileId = ZCFile.getInstance().getFolderInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).uploadFile(file).getFileId().toString();

        if (deleteSource) {
            Files.delete(file.toPath());
        }
        return fileId;
    }

    public static File downloadAsset(String fileId, String fileName) throws Exception {
        File file = DiskFileService.createFile(fileName);
        try (InputStream inputStream = ZCFile.getInstance().getFolderInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).downloadFile(Long.parseLong(fileId))) {
            Files.copy(inputStream, file.toPath());
        }
        return file;
    }

    public static void deleteAsset(String fileId) throws Exception {
        ZCFile.getInstance().getFolderInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT).deleteFile(Long.parseLong(fileId));
    }

    public static void persistJobDetail(JobDetail jobDetail) throws Exception {
        File paramsFile = createParamsFile(jobDetail.getParams());
        String oldParamsFileId = jobDetail.getParamsFileId();
        String newParamsFileId = uploadAsset(paramsFile, true);

        jobDetail.setParamsFileId(newParamsFileId);
        jobDetail.setParamsFileName(paramsFile.getName());

        updateJobDetail(jobDetail);
        deleteAsset(oldParamsFileId);

    }

}
