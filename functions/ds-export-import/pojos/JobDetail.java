package pojos;

import com.zc.component.object.ZCRowObject;
import enums.JobOperation;
import enums.JobStatus;
import tables.DatastoreExportImportJobDetailsTable;

import java.util.HashMap;


public class JobDetail {
    private String rowId;
    private String status;
    private String message;
    private String operation;
    private String createdTime;
    private String paramsFileId;
    private String paramsFileName;
    private JobDetailParam params;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(String createdTime) {
        this.createdTime = createdTime;
    }

    public String getParamsFileId() {
        return paramsFileId;
    }

    public void setParamsFileId(String paramsFileId) {
        this.paramsFileId = paramsFileId;
    }

    public String getParamsFileName() {
        return paramsFileName;
    }

    public void setParamsFileName(String paramsFileName) {
        this.paramsFileName = paramsFileName;
    }

    public JobDetailParam getParams() {
        return params;
    }

    public void setParams(JobDetailParam params) {
        this.params = params;
    }

    public ZCRowObject getInsertPayload() {
        ZCRowObject zcRowObject = ZCRowObject.getInstance();

        zcRowObject.set(DatastoreExportImportJobDetailsTable.STATUS.Raw.value, status);
        zcRowObject.set(DatastoreExportImportJobDetailsTable.MESSAGE.Raw.value, message);
        zcRowObject.set(DatastoreExportImportJobDetailsTable.OPERATION.Raw.value, operation);
        zcRowObject.set(DatastoreExportImportJobDetailsTable.PARAMS_FILE_ID.Raw.value, paramsFileId);
        zcRowObject.set(DatastoreExportImportJobDetailsTable.PARAMS_FILE_NAME.Raw.value, paramsFileName);

        return zcRowObject;
    }

    public void loadFromQueryResult(ZCRowObject zcRowObject) {
        this.rowId = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.ROWID.Raw.value));
        this.status = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.STATUS.Raw.value));
        this.message = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.MESSAGE.Raw.value));
        this.operation = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.OPERATION.Raw.value));
        this.createdTime = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.CREATEDTIME.Raw.value));
        this.paramsFileId = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.PARAMS_FILE_ID.Raw.value));
        this.paramsFileName = String.valueOf(zcRowObject.get(DatastoreExportImportJobDetailsTable.NAME, DatastoreExportImportJobDetailsTable.PARAMS_FILE_NAME.Raw.value));
    }

    public HashMap<String, Object> getResponseMap() {
        HashMap<String, Object> hashMap = new HashMap<>();

        hashMap.put("id", rowId);
        hashMap.put("message", message);
        hashMap.put("created_time", createdTime);
        hashMap.put("status", JobStatus.getJobStatusByValue(this.status).mappedValue);
        hashMap.put("operation", JobOperation.getJobOperationByValue(operation).mappedValue);

        if (params != null) {
            hashMap.put("params", params);
        }

        return hashMap;
    }


}
