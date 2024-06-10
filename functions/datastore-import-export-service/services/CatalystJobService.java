package services;

import com.zc.component.circuits.ZCCircuit;
import com.zc.component.circuits.ZCCircuitDetails;

import org.json.simple.JSONObject;

import java.util.HashMap;

public class CatalystJobService {
    public static void runImportExportJob(String jobId, String domain) throws Exception{
        HashMap<String,String> params = new HashMap<>();
        params.put("jobId",jobId);
        params.put("domain",domain);

        ZCCircuitDetails zcCircuitDetails = ZCCircuit.getInstance().getCircuitInstance(12130000005193029L);
        zcCircuitDetails.execute("DATASTORE_EXPORT_" + System.currentTimeMillis(), new JSONObject(params));
    }
}
