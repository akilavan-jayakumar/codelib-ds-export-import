package services;

import com.zc.component.circuits.ZCCircuit;
import com.zc.component.circuits.ZCCircuitDetails;
import org.json.simple.JSONObject;

import java.util.HashMap;

public class CatalystJobService {
    public static void createExportJob(HashMap<String,String> params) throws Exception{
        ZCCircuitDetails zcCircuitDetails = ZCCircuit.getInstance().getCircuitInstance(12130000005193029L);
        zcCircuitDetails.execute("DATASTORE_EXPORT_" + System.currentTimeMillis(), new JSONObject(params));
    }

    public static void endExportJob(HashMap<String,String> params) throws Exception{
        ZCCircuitDetails zcCircuitDetails = ZCCircuit.getInstance().getCircuitInstance(12130000005193029L);
        zcCircuitDetails.execute("DATASTORE_EXPORT_" + System.currentTimeMillis(), new JSONObject(params));
    }
}
