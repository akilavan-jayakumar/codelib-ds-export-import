package tablefilters;

import com.catalystsolutions.querybuilder.criterias.StringListColumnCriteria;
import tables.DatastoreExportImportJobDetailsTable;

public class DatastoreExportImportJobDetailsTableFilter {
    public final StringListColumnCriteria rowIds;
    public final StringListColumnCriteria statuses;
    public final StringListColumnCriteria operations;

    private DatastoreExportImportJobDetailsTableFilter(){
        this.rowIds = StringListColumnCriteria.getInstance(DatastoreExportImportJobDetailsTable.ROWID);
        this.statuses = StringListColumnCriteria.getInstance(DatastoreExportImportJobDetailsTable.STATUS);
        this.operations = StringListColumnCriteria.getInstance(DatastoreExportImportJobDetailsTable.OPERATION);
    }

    public static DatastoreExportImportJobDetailsTableFilter getInstance(){
        return new DatastoreExportImportJobDetailsTableFilter();
    }
}
