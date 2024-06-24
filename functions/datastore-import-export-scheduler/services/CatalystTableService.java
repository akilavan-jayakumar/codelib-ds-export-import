package services;

import com.catalystsolutions.querybuilder.builders.QueryBuilder;
import com.catalystsolutions.querybuilder.enums.Operation;
import com.catalystsolutions.querybuilder.pojos.Column;
import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowObject;
import com.zc.component.zcql.ZCQL;
import constants.CatalystTableConstants;

import java.util.List;
import java.util.Map;

public class CatalystTableService {
    private final String tableName;

    public CatalystTableService(String tableName) {
        this.tableName = tableName;
    }

    private List<ZCRowObject> getZCRowObjectsFromMaps(List<Map<String, String>> records) {
        return records.stream().map(record -> {
            ZCRowObject zcRowObject = ZCRowObject.getInstance();

            for (String key : record.keySet()) {
                zcRowObject.set(key, record.get(key));
            }

            return zcRowObject;
        }).toList();
    }

    public Long getTotalRecords() throws Exception {
        Column column = new Column(tableName, CatalystTableConstants.SystemColumns.ROWID);

        QueryBuilder queryBuilder = QueryBuilder.getInstance(Operation.SELECT);
        queryBuilder.count(column);
        queryBuilder.from(tableName);

        List<ZCRowObject> records = ZCQL.getInstance().executeQuery(queryBuilder.build());
        return Long.parseLong(records.get(0).get(tableName, column.Raw.count).toString());
    }

    public void insertRecords(List<Map<String, String>> records) throws Exception {
        List<ZCRowObject> zcRowObjects = ZCObject.getInstance().getTableInstance(tableName).insertRows(getZCRowObjectsFromMaps(records));

        for (int i = 0; i < zcRowObjects.size(); i++) {
            records.get(i).put(CatalystTableConstants.SystemColumns.ROWID, zcRowObjects.get(i).get(CatalystTableConstants.SystemColumns.ROWID).toString());
        }
    }

    public void updateRecords(List<Map<String, String>> records) throws Exception {
        ZCObject.getInstance().getTableInstance(tableName).updateRows(getZCRowObjectsFromMaps(records));
    }
}
