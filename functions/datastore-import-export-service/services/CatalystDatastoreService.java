package services;

import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCTable;
import com.zc.component.object.bulk.*;
import tables.DatastoreImportExportJobDetailsTable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class CatalystDatastoreService {

    public static List<ZCTable> getAllTables() throws Exception {
        return ZCObject.getInstance().getAllTables().stream().filter(obj -> !obj.getName().equals(DatastoreImportExportJobDetailsTable.NAME)).toList();
    }

    public static File downloadBulkReadReport(String bulkReadJobId, String fileName) throws Exception {
        File file = DiskFileService.createFile(fileName);

        try (InputStream inputStream = ZCDataStoreBulk.getInstance().getBulkReadInstance().downloadBulkReadJobReport(Long.parseLong(bulkReadJobId))) {
            ZipInputStream zipInputStream = new ZipInputStream(inputStream);
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    if (zipEntry.isDirectory()) {
                        throw new Exception("Downloaded report zip contains a directory");
                    }
                    int bytesRead;
                    byte[] buffer = new byte[8 * 1024];
                    while ((bytesRead = zipInputStream.read(buffer)) > 0) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
            }

            return file;
        }
    }


    public static String createBulkRead(String table, int page, String callbackUrl, HashMap<String, String> headers) throws Exception {
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
