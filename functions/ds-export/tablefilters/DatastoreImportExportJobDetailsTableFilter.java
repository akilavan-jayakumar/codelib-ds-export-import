package tablefilters;

import com.catalystsolutions.querybuilder.criterias.StringListColumnCriteria;
import tables.DatastoreImportExportJobDetailsTable;

public class DatastoreImportExportJobDetailsTableFilter {
    public final StringListColumnCriteria rowIds;
    public final StringListColumnCriteria statuses;
    public final StringListColumnCriteria operations;

    private DatastoreImportExportJobDetailsTableFilter() {
        this.rowIds = StringListColumnCriteria.getInstance(DatastoreImportExportJobDetailsTable.ROWID);
        this.statuses = StringListColumnCriteria.getInstance(DatastoreImportExportJobDetailsTable.STATUS);
        this.operations = StringListColumnCriteria.getInstance(DatastoreImportExportJobDetailsTable.OPERATION);
    }

    public static DatastoreImportExportJobDetailsTableFilter getInstance() {
        return new DatastoreImportExportJobDetailsTableFilter();
    }
}
