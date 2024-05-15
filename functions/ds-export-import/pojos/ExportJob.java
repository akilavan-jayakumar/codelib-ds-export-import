package pojos;

import java.util.UUID;

public class ExportJob {
    private Integer page;
    private String table;
    private String jobId;
    private Integer status;
    private final String id;

    public ExportJob() {
        this.id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
