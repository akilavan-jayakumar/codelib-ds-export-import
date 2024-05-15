package services;

import com.zc.component.circuits.ZCCircuit;
import com.zc.component.circuits.ZCCircuitDetails;
import org.json.simple.JSONObject;

import java.util.HashMap;

public class CatalystJobService {
    public static void createImportJob(HashMap<String, String> params) throws Exception {
        ZCCircuitDetails zcCircuitDetails = ZCCircuit.getInstance().getCircuitInstance(12130000005496017L);
        zcCircuitDetails.execute("DATASTORE_IMPORT_" + System.currentTimeMillis(), new JSONObject(params));
    }

}
