package tables;

import com.catalystsolutions.querybuilder.pojos.Column;


public final class DatastoreImportExportJobDetailsTable {
    public static final String NAME = "DatastoreImportExportJobDetails";
    public static final Column ROWID = new Column(NAME, "ROWID");
    public static final Column STATUS = new Column(NAME, "STATUS");
    public static final Column MESSAGE = new Column(NAME, "MESSAGE");
    public static final Column OPERATION = new Column(NAME, "OPERATION");
    public static final Column CREATEDTIME = new Column(NAME, "CREATEDTIME");
    public static final Column ASSET_FILE_ID = new Column(NAME, "OUTPUT_FILE_ID");
    public static final Column PARAMS_FILE_ID = new Column(NAME, "PARAMS_FILE_ID");
    public static final Column ASSET_FILE_NAME = new Column(NAME, "OUTPUT_FILE_NAME");
    public static final Column PARAMS_FILE_NAME = new Column(NAME, "PARAMS_FILE_NAME");
}
