package services;

import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowObject;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;


public class CatalystTableService {
    private static final Logger LOGGER = Logger.getLogger(CatalystTableService.class.getName());
    private final String table;

    public CatalystTableService(String table) {
        this.table = table;
    }

    @SuppressWarnings("unchecked")
    private List<HashMap<String, String>> convertRecordsToHashMap(List<JSONObject> jsonObjects) throws Exception {
        List<HashMap<String, String>> records = new ArrayList<>();

        for (JSONObject jsonObject : jsonObjects) {
            HashMap<String, String> hashMap = new HashMap<>();
            jsonObject.forEach((key, value) -> {
                hashMap.put(key.toString(), String.valueOf(value));
            });

            records.add(hashMap);
        }

        return records;
    }

    public List<HashMap<String, String>> insertRecords(List<ZCRowObject> zcRowObjects) throws Exception {
        List<JSONObject> jsonObjects = ZCObject.getInstance().getTable(table).insertRows(zcRowObjects).stream().map(ZCRowObject::getRowObject).toList();
        return this.convertRecordsToHashMap(jsonObjects);
    }

    public void updateRecords(List<ZCRowObject> zcRowObjects) throws Exception {
        ZCObject.getInstance().getTable(table).updateRows(zcRowObjects);
    }


}
