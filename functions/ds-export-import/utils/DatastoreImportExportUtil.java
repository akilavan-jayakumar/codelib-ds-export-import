package utils;

import com.zc.component.object.ZCColumn;
import com.zc.component.object.ZCTable;
import pojos.Column;
import pojos.JobDetailParam;
import pojos.ParentTable;
import pojos.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DatastoreImportExportUtil {
    public static JobDetailParam convertZCTablesToJobDetailsParams(List<ZCTable> zcTables) throws Exception {
        List<Table> tables = new ArrayList<>();
        HashMap<String, String> tableIdNameMapping = new HashMap<>();
        HashMap<String, String> columnIdNameMapping = new HashMap<>();

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
        JobDetailParam jobDetailParam = new JobDetailParam();
        jobDetailParam.setTables(tables);

        return jobDetailParam;
    }
}
