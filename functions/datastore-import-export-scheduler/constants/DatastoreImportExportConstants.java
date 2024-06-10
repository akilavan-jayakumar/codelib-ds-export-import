package constants;

public class DatastoreImportExportConstants {
    public static final Long OPERATION_DELAY = 1000L;
    public static final String EXPORT_ZIP_FILE_PREFIX = "export";
    public static final String TABLE_META_JSON = "table-meta.json";
    public static final Integer DEPENDENT_TABLE_CSV_MAX_RECORDS = 30000;
    public static final Integer INDEPENDENT_TABLE_CSV_MAX_RECORDS = 100000;
    public static final String PARAMS_JSON_SUFFIX = "job-detail-params.json";
    public static final String MAPPING_JSON_SUFFIX = "mapping.json";
    public static final Integer DEPENDENT_TABLE_MAX_COLUMNS_PER_UPDATE = 10;
}
