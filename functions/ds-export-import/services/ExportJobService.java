package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.CatalystCacheSegments;
import pojos.ExportJob;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class ExportJobService {


    private static void replaceJobs(List<ExportJob> exportJobs) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS, objectMapper.writeValueAsString(exportJobs));
    }

    public static List<ExportJob> getAllJobs() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
        String jobs = catalystCacheService.getValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS);
        return objectMapper.readValue(jobs, new TypeReference<List<ExportJob>>() {
        });
    }

    public static File getExportReport(String jobId, String folder, String fileName) throws Exception {
        CatalystDatastoreService catalystDatastoreService = new CatalystDatastoreService();
        return new File("");
    }

    public static void createJobs(List<ExportJob> exportJobs) throws Exception {
        List<ExportJob> allJobs = getAllJobs();
        allJobs.addAll(exportJobs);
        replaceJobs(allJobs);

    }

    public static String executeJob(ExportJob exportJob, String callbackUrl, HashMap<String, String> headers) throws Exception {
        CatalystTableService catalystTableService = new CatalystTableService(exportJob.getTable());
        return catalystTableService.createBulkExport(exportJob.getPage(), callbackUrl, headers);
    }

    public static void updateJobs(List<ExportJob> exportJobs) throws Exception {
        List<ExportJob> allJobs = getAllJobs();
        for (ExportJob newExportJob : exportJobs) {
            for (ExportJob oldExportJob : allJobs) {
                if (oldExportJob.getId().equals(newExportJob.getId())) {
                    oldExportJob.setJobId(newExportJob.getJobId());
                    oldExportJob.setStatus(newExportJob.getStatus());
                }
            }
        }
        replaceJobs(allJobs);

    }


}
