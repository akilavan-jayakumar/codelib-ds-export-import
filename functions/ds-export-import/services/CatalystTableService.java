package services;

import com.zc.component.object.bulk.ZCBulkCallbackDetails;
import com.zc.component.object.bulk.ZCBulkQueryDetails;
import com.zc.component.object.bulk.ZCBulkReadServices;
import com.zc.component.object.bulk.ZCDataStoreBulk;
import com.zc.component.object.bulk.result.ZCBulkResult;

import java.util.HashMap;
import java.util.logging.Logger;

public class CatalystTableService {
    private static final Logger LOGGER = Logger.getLogger(CatalystTableService.class.getName());
    private final String table;


    public CatalystTableService(String table) {
        this.table = table;
    }


    public String createBulkExport(int page, String callbackUrl, HashMap<String, String> headers) throws Exception {
        ZCBulkCallbackDetails zcBulkCallbackDetails = ZCBulkCallbackDetails.getInstance();
        zcBulkCallbackDetails.setUrl(callbackUrl);
        zcBulkCallbackDetails.setHeadersMap(headers);

        ZCBulkQueryDetails zcBulkQueryDetails = ZCBulkQueryDetails.getInstance();
        zcBulkQueryDetails.setPage(page);


        ZCBulkReadServices zcBulkReadServices = ZCDataStoreBulk.getInstance().getBulkReadInstance();

        return zcBulkReadServices.createBulkReadJob(table, zcBulkQueryDetails, zcBulkCallbackDetails).getJobId().toString();
    }


}
