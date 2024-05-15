package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import constants.CatalystCacheSegments;
import pojos.ImportJob;

import java.util.List;

public class ImportJobService {

    private static void replaceImportJobs(List<ImportJob> importJobs) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME);
        catalystCacheService.putValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_JOBS, objectMapper.writeValueAsString(importJobs));
    }

    public static List<ImportJob> getAllImportJobs() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        CatalystCacheService catalystCacheService = new CatalystCacheService(CatalystCacheSegments.DS_EXPORT_IMPORT.NAME);
        return objectMapper.readValue(catalystCacheService.getValue(CatalystCacheSegments.DS_EXPORT_IMPORT.Keys.DS_IMPORT_JOBS, "[]"), new TypeReference<List<ImportJob>>() {
        });
    }

    public static void createImportJobs(List<ImportJob> importJobs) throws Exception {
        List<ImportJob> allImportJobs = getAllImportJobs();
        allImportJobs.addAll(importJobs);
        replaceImportJobs(allImportJobs);
    }

    public static void updateImportJobs(List<ImportJob> importJobs) throws Exception {
        List<ImportJob> allImportJobs = getAllImportJobs();

        for (ImportJob importJob : importJobs) {
            ImportJob targetJob = allImportJobs.stream().filter(obj -> obj.getId().equals(importJob.getId())).findAny().orElse(null);
            if (targetJob != null) {
                targetJob.setStatus(importJob.getStatus());
                targetJob.setFile_id(importJob.getFile_id());
            }
            replaceImportJobs(allImportJobs);
        }
    }

}
