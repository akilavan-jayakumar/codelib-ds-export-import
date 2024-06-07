package pojos;

import com.zc.component.object.ZCRowObject;
import tables.DatastoreImportExportJobDetailsTable;


public class JobDetail {
    private String id;
    private Integer status;
    private String message;
    private Integer operation;
    private String createdTime;
    private String paramsFileId;
    private String assetFileId;
    private String paramsFileName;
    private String assetFileName;
    private JobDetailParam params;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getOperation() {
        return operation;
    }

    public void setOperation(Integer operation) {
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

    public String getAssetFileId() {
        return assetFileId;
    }

    public void setAssetFileId(String assetFileId) {
        this.assetFileId = assetFileId;
    }

    public String getParamsFileName() {
        return paramsFileName;
    }

    public void setParamsFileName(String paramsFileName) {
        this.paramsFileName = paramsFileName;
    }

    public String getAssetFileName() {
        return assetFileName;
    }

    public void setAssetFileName(String assetFileName) {
        this.assetFileName = assetFileName;
    }

    public JobDetailParam getParams() {
        return params;
    }

    public void setParams(JobDetailParam params) {
        this.params = params;
    }


    public ZCRowObject getUpdatePayload() {
        ZCRowObject zcRowObject = ZCRowObject.getInstance();

        zcRowObject.set(DatastoreImportExportJobDetailsTable.ROWID.Raw.value, id);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.STATUS.Raw.value, status);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.MESSAGE.Raw.value, message);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.OPERATION.Raw.value, operation);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.ASSET_FILE_ID.Raw.value, paramsFileId);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.OUTPUT_FILE_ID.Raw.value, assetFileId);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.PARAMS_FILE_NAME.Raw.value, paramsFileName);
        zcRowObject.set(DatastoreImportExportJobDetailsTable.ASSET_FILE_NAME.Raw.value, assetFileName);

        return zcRowObject;
    }

    public void loadFromQueryResult(ZCRowObject zcRowObject) {
        this.id = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.ROWID.Raw.value));
        this.message = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.MESSAGE.Raw.value));
        this.createdTime = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.CREATEDTIME.Raw.value));
        this.paramsFileId = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.ASSET_FILE_ID.Raw.value));
        this.assetFileId = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.OUTPUT_FILE_ID.Raw.value));
        this.paramsFileName = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.PARAMS_FILE_NAME.Raw.value));
        this.assetFileName = String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.ASSET_FILE_NAME.Raw.value));

        this.status = Integer.parseInt(String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.STATUS.Raw.value)));
        this.operation = Integer.parseInt(String.valueOf(zcRowObject.get(DatastoreImportExportJobDetailsTable.NAME, DatastoreImportExportJobDetailsTable.OPERATION.Raw.value)));
    }


}
