package services;

import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowObject;

import java.util.List;
import java.util.logging.Logger;

public class CatalystTableService {
    private static final Logger LOGGER = Logger.getLogger(CatalystTableService.class.getName());
    private final String table;


    public CatalystTableService(String table) {
        this.table = table;
    }

    public List<ZCRowObject> insertRecords(List<ZCRowObject> zcRowObjects) throws Exception {
        return ZCObject.getInstance().getTable(table).insertRows(zcRowObjects);
    }


}
