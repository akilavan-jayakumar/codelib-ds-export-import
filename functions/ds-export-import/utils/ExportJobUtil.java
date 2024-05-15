package utils;

import constants.DatastoreExportImportUrlConstants;
import enums.JobStatus;
import pojos.ExportJob;
import processors.CsvProcessor;
import services.ExportJobService;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ExportJobUtil {

    private static final String CSV_FILES_DIR = "csv-files";
    private static final Integer MAX_RECORDS_IN_CSV = 30000;

    private static String getCsvFileName(ExportJob exportJob, String suffix) {
        String baseName = exportJob.getTable().replaceAll(" ", "") + "-" + exportJob.getPage();
        if (!suffix.isEmpty()) {
            return baseName + "-" + suffix + ".csv";
        } else {
            return baseName + ".csv";
        }

    }

    public static List<ExportJob> getAllJobs() throws Exception {
        return ExportJobService.getAllJobs();
    }

    public static ExportJob getJobByJobId(String jobId) throws Exception {
        return getAllJobs().stream().filter(obj -> obj.getJobId().equals(jobId)).findAny().orElse(null);
    }

    public static List<ExportJob> getAllJobs(List<JobStatus> statuses) throws Exception {
        List<ExportJob> exportJobs = getAllJobs();
        List<Integer> statusValues = statuses.stream().map(obj -> obj.value).toList();

        return exportJobs.stream().filter(obj -> statusValues.contains(obj.getStatus())).toList();
    }

    public static void createJob(ExportJob exportJob) throws Exception {
        ExportJobService.createJobs(Collections.singletonList(exportJob));
    }

    public static void executeJob(ExportJob exportJob, String domain) throws Exception {
        String jobId = ExportJobService.executeJob(exportJob, domain + DatastoreExportImportUrlConstants.SERVER + DatastoreExportImportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreExportImportUrlConstants.EXPORT + DatastoreExportImportUrlConstants.CALLBACK, new HashMap<>());
        exportJob.setJobId(jobId);
        exportJob.setStatus(JobStatus.RUNNING.value);
        updateJob(exportJob);
    }

    public static void updateJob(ExportJob exportJob) throws Exception {
        ExportJobService.updateJobs(Collections.singletonList(exportJob));
    }

    public static List<File> getExportJobReports(ExportJob exportJob) throws Exception {
        List<File> exportJobReports = new ArrayList<>();
        File masterFile = ExportJobService.getExportReport(exportJob.getJobId(), CSV_FILES_DIR, getCsvFileName(exportJob, ""));
        CsvProcessor csvProcessor = new CsvProcessor(masterFile);


        int totalRecords = csvProcessor.getTotalRecords();
        if (totalRecords > MAX_RECORDS_IN_CSV) {
            int totalFiles = (int) Math.ceil((double) totalRecords / MAX_RECORDS_IN_CSV);


            for (int i = 1; i <= totalFiles; i++) {
                int start = (i - 1) * MAX_RECORDS_IN_CSV;
                int end = start + MAX_RECORDS_IN_CSV;

                exportJobReports.add(csvProcessor.slice(start, end, CSV_FILES_DIR, getCsvFileName(exportJob, String.valueOf(i))));

            }
            Files.delete(masterFile.toPath());
        } else {
            exportJobReports.add(masterFile);
        }

        return exportJobReports;
    }


}
