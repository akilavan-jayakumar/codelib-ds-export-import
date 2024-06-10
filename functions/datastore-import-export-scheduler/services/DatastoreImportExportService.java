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
import com.zc.component.files.ZCFolder;
import com.zc.component.object.ZCColumn;
import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowObject;
import com.zc.component.object.ZCTable;
import com.zc.component.zcql.ZCQL;
import constants.CatalystFilestoreFolders;
import constants.DatastoreImportExportConstants;
import pojos.*;
import tablefilters.DatastoreImportExportJobDetailsTableFilter;
import tables.DatastoreImportExportJobDetailsTable;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

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
        queryBuilder.select(DatastoreImportExportJobDetailsTable.ROWID, DatastoreImportExportJobDetailsTable.STATUS, DatastoreImportExportJobDetailsTable.MESSAGE, DatastoreImportExportJobDetailsTable.OPERATION, DatastoreImportExportJobDetailsTable.CREATEDTIME, DatastoreImportExportJobDetailsTable.ASSET_FILE_ID, DatastoreImportExportJobDetailsTable.OUTPUT_FILE_ID, DatastoreImportExportJobDetailsTable.PARAMS_FILE_NAME, DatastoreImportExportJobDetailsTable.ASSET_FILE_NAME);
        queryBuilder.from(DatastoreImportExportJobDetailsTable.NAME);
        queryBuilder.where(criteriaBuilder.build());
        queryBuilder.orderBy(orderByBuilder.build());
        queryBuilder.limit(limit);
        queryBuilder.offset(offset);


        List<ZCRowObject> zcRowObjects = ZCQL.getInstance().executeQuery(queryBuilder.build());
        return convertZCRowObjectsToJobDetails(zcRowObjects);

    }


    public static JobDetail getJobDetailWithParamsById(String jobId) throws Exception {
        DatastoreImportExportJobDetailsTableFilter datastoreImportExportJobDetailsTableFilter = createDatastoreExportImportJobDetailsTableFilter(List.of(jobId), Collections.emptyList(), Collections.emptyList());
        JobDetail jobDetail = getJobDetailsByLimit(1, 1, datastoreImportExportJobDetailsTableFilter).stream().findAny().orElse(null);

        if (jobDetail == null) {
            return null;
        }

        JobDetailParam jobDetailParam = getJobDetailParams(jobDetail.getParamsFileId(), jobDetail.getParamsFileName());
        jobDetail.setParams(jobDetailParam);


        return jobDetail;
    }


    public static JobDetailParam getJobDetailParams(String paramsFileId, String paramsFileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = downloadAsset(paramsFileId, paramsFileName);
        String json = DiskFileService.readFile(file);

        return objectMapper.readValue(json, new TypeReference<JobDetailParam>() {
        });
    }

    public static Map<String, String> getOldToNewIdMappings(String mappingFileId, String mappingFileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        File file = downloadAsset(mappingFileId, mappingFileName);
        String json = DiskFileService.readFile(file);

        return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {
        });
    }

    public static File generateTableMetaJsonFromTables(List<Table> tables) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return DiskFileService.writeFile(DatastoreImportExportConstants.TABLE_META_JSON, objectMapper.writeValueAsString(tables));
    }

    public static List<Table> generateTablesFromTableMetaJsonFile(File file) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String content = DiskFileService.readFile(file);
        return objectMapper.readValue(content, new TypeReference<List<Table>>() {
        });
    }

    public static List<Table> generateTablesFromZCTables() throws Exception {
        List<Table> tables = new ArrayList<>();
        HashMap<String, String> tableIdNameMapping = new HashMap<>();
        HashMap<String, String> columnIdNameMapping = new HashMap<>();

        List<ZCTable> zcTables = CatalystDatastoreService.getAllTables();

        for (ZCTable zcTable : zcTables) {
            List<Column> columns = new ArrayList<>();

            Table table = new Table(zcTable.getName());
            tableIdNameMapping.put(zcTable.getTableId().toString(), zcTable.getName());

            for (ZCColumn zcColumn : zcTable.getAllColumns()) {
                columnIdNameMapping.put(zcColumn.getColumnId().toString(), zcColumn.getColumnName());
                Column column = new Column(zcColumn.getColumnName(), zcColumn.getDataType());

                if (zcColumn.getParentTable() != null) {
                    ParentTable parentTable = new ParentTable();
                    parentTable.setTable(zcColumn.getParentTable());
                    parentTable.setColumn(zcColumn.getParentColumn());
                    column.setParent(parentTable);
                }

                columns.add(column);
            }

            table.setColumns(columns);
            tables.add(table);
            Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);
        }

        for (Table table : tables) {
            for (Column column : table.getColumns()) {
                if (column.getParent() != null) {
                    String tableId = column.getParent().getTable();
                    String columnId = column.getParent().getColumn();

                    column.getParent().setTable(tableIdNameMapping.get(tableId));
                    column.getParent().setColumn(columnIdNameMapping.get(columnId));
                }
            }
        }
        return tables;
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
        stringJoiner.add(DatastoreImportExportConstants.PARAMS_JSON_SUFFIX);

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

    public static void deleteAssets(List<String> fileIds) throws Exception {
        ZCFolder zcFolder = ZCFile.getInstance().getFolderInstance(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT);
        for (String fileId : fileIds) {
            zcFolder.deleteFile(Long.parseLong(fileId));
            Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);
        }
    }


    public static void deleteAsset(String fileId) throws Exception {
        deleteAssets(Collections.singletonList(fileId));
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

    public static String persistOldToNewIdMapping(Map<String, String> oldToNewIdMapping, String fileName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = DiskFileService.writeFile(fileName, objectMapper.writeValueAsString(oldToNewIdMapping));
        return uploadAsset(file, true);
    }

}
