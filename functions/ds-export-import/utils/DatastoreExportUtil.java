package utils;

import constants.CatalystCacheSegments;
import enums.ExportAction;
import services.CatalystCacheService;
import services.CatalystJobService;

import java.util.HashMap;

public class DatastoreExportUtil {
    public static boolean isDatastoreExporting() throws Exception {
        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);

        return catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS) != null;
    }

    public static void startExport(HashMap<String, String> params) throws Exception {
        params.put("action", ExportAction.START.value);
        CatalystJobService.createExportJob(params);
    }

    public static void endExport() throws Exception {
        HashMap<String, String> params = new HashMap<>();
        params.put("action", ExportAction.END.value);

        CatalystJobService.endExportJob(params);
    }


}
