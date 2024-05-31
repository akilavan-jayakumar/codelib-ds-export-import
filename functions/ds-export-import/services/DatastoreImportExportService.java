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
import tablefilters.DatastoreExportImportJobDetailsTableFilter;
import tables.DatastoreExportImportJobDetailsTable;

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

    private static DatastoreExportImportJobDetailsTableFilter createDatastoreExportImportJobDetailsTableFilter(List<String> rowIds, List<String> statuses, List<String> operations) {
        DatastoreExportImportJobDetailsTableFilter datastoreExportImportJobDetailsTableFilter = DatastoreExportImportJobDetailsTableFilter.getInstance();

        if (!rowIds.isEmpty()) {
            datastoreExportImportJobDetailsTableFilter.rowIds.addAll(rowIds.stream().map(QueryUtil::toQueryString).toList());
        }

        if (!statuses.isEmpty()) {
            datastoreExportImportJobDetailsTableFilter.statuses.addAll(statuses.stream().map(QueryUtil::toQueryString).toList());
        }

        if (!operations.isEmpty()) {
            datastoreExportImportJobDetailsTableFilter.operations.addAll(operations.stream().map(QueryUtil::toQueryString).toList());
        }


        return datastoreExportImportJobDetailsTableFilter;
    }

    private static List<JobDetail> getJobDetailsByLimit(Integer offset, Integer limit, DatastoreExportImportJobDetailsTableFilter datastoreExportImportJobDetailsTableFilter) throws Exception {
        OrderByBuilder orderByBuilder = OrderByBuilder.getInstance();
        orderByBuilder.add(OrderBy.getInstance(DatastoreExportImportJobDetailsTable.CREATEDTIME, SortOrder.DESC));

        CriteriaBuilder criteriaBuilder = CriteriaBuilder.getInstance();
        criteriaBuilder.add(datastoreExportImportJobDetailsTableFilter.rowIds);
        criteriaBuilder.add(datastoreExportImportJobDetailsTableFilter.statuses);
        criteriaBuilder.add(datastoreExportImportJobDetailsTableFilter.operations);

        QueryBuilder queryBuilder = QueryBuilder.getInstance(Operation.SELECT);
        queryBuilder.select(DatastoreExportImportJobDetailsTable.ROWID, DatastoreExportImportJobDetailsTable.STATUS, DatastoreExportImportJobDetailsTable.MESSAGE, DatastoreExportImportJobDetailsTable.OPERATION, DatastoreExportImportJobDetailsTable.CREATEDTIME, DatastoreExportImportJobDetailsTable.PARAMS_FILE_ID, DatastoreExportImportJobDetailsTable.PARAMS_FILE_NAME);
        queryBuilder.from(DatastoreExportImportJobDetailsTable.NAME);
        queryBuilder.where(criteriaBuilder.build());
        queryBuilder.orderBy(orderByBuilder.build());
        queryBuilder.limit(limit);
        queryBuilder.offset(offset);


        List<ZCRowObject> zcRowObjects = ZCQL.getInstance().executeQuery(queryBuilder.build());
        return convertZCRowObjectsToJobDetails(zcRowObjects);

    }

    public static List<JobDetail> getJobDetailsByLimit(Integer offset, Integer limit, List<String> statuses, List<String> operations) throws Exception {
        DatastoreExportImportJobDetailsTableFilter datastoreExportImportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(Collections.emptyList(), statuses, operations);
        return getJobDetailsByLimit(offset, limit, datastoreExportImportJobDetailsTableFilter);
    }

    public static JobDetail getJobDetailById(String rowId) throws Exception {
        DatastoreExportImportJobDetailsTableFilter datastoreExportImportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(List.of(rowId), Collections.emptyList(), Collections.emptyList());
        return getJobDetailsByLimit(1, 1, datastoreExportImportJobDetailsTableFilter).stream().findAny().orElse(null);
    }

    public static JobDetail getPendingOrRunningJobDetail() throws Exception {
        DatastoreExportImportJobDetailsTableFilter datastoreExportImportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(Collections.emptyList(), List.of(JobStatus.PENDING.value, JobStatus.RUNNING.value), Collections.emptyList());
        return getJobDetailsByLimit(1, 1, datastoreExportImportJobDetailsTableFilter).stream().findAny().orElse(null);
    }

    public static JobDetailParam getJobDetailParams(String paramsFileId, String paramsFileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = downloadAsset(paramsFileId, paramsFileName);
        String json = DiskFileService.readFile(file);

        return objectMapper.readValue(json, new TypeReference<JobDetailParam>() {
        });
    }

    public static void createJobDetails(List<JobDetail> jobDetails) throws Exception {
        List<ZCRowObject> rowObjects = ZCObject.getInstance().getTable(DatastoreExportImportJobDetailsTable.NAME).insertRows(jobDetails.stream().map(JobDetail::getInsertPayload).toList());

        for (int i = 0; i < rowObjects.size(); i++) {
            jobDetails.get(i).setRowId(rowObjects.get(i).get(DatastoreExportImportJobDetailsTable.ROWID.Raw.value).toString());
        }
    }

    public static void createJobDetail(JobDetail jobDetail) throws Exception {
        createJobDetails(List.of(jobDetail));
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


}
