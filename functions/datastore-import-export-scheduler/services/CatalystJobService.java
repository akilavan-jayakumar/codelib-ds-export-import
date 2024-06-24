package services;

import com.zc.component.circuits.ZCCircuit;
import com.zc.component.circuits.ZCCircuitDetails;
import constants.EnvConstants;
import org.json.simple.JSONObject;

import java.util.HashMap;

public class CatalystJobService {
    public static void runImportExportJob(String jobId, String domain) throws Exception{
        HashMap<String,String> params = new HashMap<>();
        params.put("jobId",jobId);
        params.put("domain",domain);

        ZCCircuitDetails zcCircuitDetails = ZCCircuit.getInstance().getCircuitInstance(Long.parseLong(EnvConstants.IMPORT_EXPORT_JOB_ID));
        zcCircuitDetails.execute("DATASTORE_EXPORT_" + System.currentTimeMillis(), new JSONObject(params));
    }
}
