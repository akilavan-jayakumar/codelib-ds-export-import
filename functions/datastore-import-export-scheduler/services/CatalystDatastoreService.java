package services;

import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCTable;
import com.zc.component.object.bulk.*;
import constants.DatastoreImportExportConstants;
import pojos.ZCTableDetail;
import tables.DatastoreImportExportJobDetailsTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CatalystDatastoreService {

    public static List<ZCTable> getAllTables() throws Exception {
        return ZCObject.getInstance().getAllTables().stream().filter(obj -> !obj.getName().equals(DatastoreImportExportJobDetailsTable.NAME)).toList();
    }

    public static List<ZCTableDetail> getAllTablesInZCTableDetail() throws Exception {
        List<ZCTableDetail> zcTableDetails = new ArrayList<>();

        List<ZCTable> zcTables = getAllTables();
        for (ZCTable zcTable : zcTables) {
            ZCTableDetail zcTableDetail = new ZCTableDetail();
            zcTableDetail.setZcTable(zcTable);
            zcTableDetail.setZcColumns(zcTable.getAllColumns());
            zcTableDetails.add(zcTableDetail);
        }

        return zcTableDetails;
    }


    public static String createBulkExport(String table, int page, String callbackUrl, HashMap<String, String> headers) throws Exception {
        ZCBulkCallbackDetails zcBulkCallbackDetails = ZCBulkCallbackDetails.getInstance();
        zcBulkCallbackDetails.setUrl(callbackUrl);
        zcBulkCallbackDetails.setHeadersMap(headers);

        ZCBulkQueryDetails zcBulkQueryDetails = ZCBulkQueryDetails.getInstance();
        zcBulkQueryDetails.setPage(page);


        ZCBulkReadServices zcBulkReadServices = ZCDataStoreBulk.getInstance().getBulkReadInstance();

        return zcBulkReadServices.createBulkReadJob(table, zcBulkQueryDetails, zcBulkCallbackDetails).getJobId().toString();
    }

    public static String createBulkWrite(String table, String fileId, String callbackUrl, HashMap<String, String> headers) throws Exception {
        ZCBulkCallbackDetails zcBulkCallbackDetails = ZCBulkCallbackDetails.getInstance();
        zcBulkCallbackDetails.setUrl(callbackUrl);
        zcBulkCallbackDetails.setHeadersMap(headers);

        ZCBulkWriteDetails zcBulkWriteDetails = ZCBulkWriteDetails.getInstance();
        zcBulkWriteDetails.setOperation(BULKWRITEOPERATION.INSERT);
        zcBulkWriteDetails.setTableIdentifier(table);
        zcBulkWriteDetails.setFileId(Long.parseLong(fileId));
        zcBulkWriteDetails.setCallback(zcBulkCallbackDetails);


        ZCBulkWriteServices zcBulkWriteServices = ZCDataStoreBulk.getInstance().getBulkWriteInstance();

        return zcBulkWriteServices.createBulkWriteJob(zcBulkWriteDetails).getJobId().toString();
    }
}
