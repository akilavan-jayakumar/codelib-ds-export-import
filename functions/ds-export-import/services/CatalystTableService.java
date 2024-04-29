package services;

import com.zc.component.object.bulk.ZCBulkCallbackDetails;
import com.zc.component.object.bulk.ZCBulkQueryDetails;
import com.zc.component.object.bulk.ZCBulkReadServices;
import com.zc.component.object.bulk.ZCDataStoreBulk;
import com.zc.component.object.bulk.result.ZCBulkResult;

import java.io.InputStream;
import java.util.HashMap;

public class CatalystTableService {
    private final String table;


    public CatalystTableService(String table) {
        this.table = table;
    }

    public ZCBulkResult createBulkExport(int page, String callbackUrl, HashMap<String, String> headers) throws Exception {
        ZCBulkCallbackDetails zcBulkCallbackDetails = ZCBulkCallbackDetails.getInstance();
        zcBulkCallbackDetails.setUrl(callbackUrl);
        zcBulkCallbackDetails.setHeadersMap(headers);

        ZCBulkQueryDetails zcBulkQueryDetails = ZCBulkQueryDetails.getInstance();
        zcBulkQueryDetails.setPage(page);


        ZCBulkReadServices zcBulkReadServices = ZCDataStoreBulk.getInstance().getBulkReadInstance();

        return zcBulkReadServices.createBulkReadJob(table, zcBulkQueryDetails, zcBulkCallbackDetails);
    }

    public InputStream downloadBulkExport(String jobId) throws Exception {
        return ZCDataStoreBulk.getInstance().getBulkReadInstance().downloadBulkReadJobReport(Long.parseLong(jobId));
    }
}
