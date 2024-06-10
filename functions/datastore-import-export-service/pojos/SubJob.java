package pojos;


import enums.JobStatus;
import enums.SubJobOperation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubJob {
    private String id;
    private String file;
    private String table;
    private Integer page;
    private Integer status;
    private String bulkJobId;
    private Integer operation;
    private List<String> columns;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getBulkJobId() {
        return bulkJobId;
    }

    public void setBulkJobId(String bulkJobId) {
        this.bulkJobId = bulkJobId;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public Integer getOperation() {
        return operation;
    }

    public void setOperation(Integer operation) {
        this.operation = operation;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public Map<String, Object> generateResponseMap() {
        Map<String, Object> map = new HashMap<>();

        map.put("id", id);
        map.put("file", file);
        map.put("page", page);
        map.put("table", table);
        map.put("columns", columns);
        map.put("status", JobStatus.getJobStatusByValue(status).mappedValue);
        map.put("bulk-job-id", bulkJobId);
        map.put("operation", SubJobOperation.getSubJobOperationByValue(operation).mappedValue);


        return map;
    }
}
