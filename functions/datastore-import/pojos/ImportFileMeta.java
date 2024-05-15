package pojos;

import java.util.UUID;

public class ImportFileMeta {
    private final String id;
    private String name;
    private String file_id;

    public ImportFileMeta() {
        this.id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    public String getFile_id() {
        return file_id;
    }

    public void setFile_id(String file_id) {
        this.file_id = file_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
