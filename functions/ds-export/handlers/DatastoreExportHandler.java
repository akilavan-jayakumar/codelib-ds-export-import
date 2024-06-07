package handlers;

import constants.DatastoreImportExportConstants;
import enums.JobStatus;
import enums.SubJobOperation;
import pojos.JobDetail;
import pojos.JobDetailParam;
import pojos.SubJob;
import pojos.Table;
import services.CatalystDatastoreService;
import services.DatastoreImportExportService;
import services.DiskFileService;
import utils.DatastoreImportExportUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class DatastoreExportHandler {
    public static void handle(JobDetail jobDetail, String domain) throws Exception {
        try {
            JobDetailParam jobDetailParam = jobDetail.getParams();
            List<Table> tables = jobDetailParam.getTables();
            List<SubJob> subJobs = jobDetailParam.getSubJobs();
            HashMap<String, String> files = (HashMap<String, String>) jobDetailParam.getFiles();

            if (jobDetail.getStatus().equals(JobStatus.PENDING.value)) {
                jobDetail.setStatus(JobStatus.RUNNING.value);

                tables.addAll(DatastoreImportExportService.generateTablesFromZCTables());

                for (Table table : tables) {
                    SubJob subJob = new SubJob();
                    subJob.setId(UUID.randomUUID().toString());
                    subJob.setTable(table.getName());
                    subJob.setPage(1);
                    subJob.setStatus(JobStatus.PENDING.value);
                    subJob.setOperation(SubJobOperation.READ.value);
                    subJobs.add(subJob);
                }

                SubJob pendingJob = subJobs.get(0);
                String bulkReadJobId = CatalystDatastoreService.createBulkExport(pendingJob.getTable(), pendingJob.getPage(), DatastoreImportExportUtil.getJobCallbackUrl(domain, jobDetail.getId()), new HashMap<>());
                pendingJob.setBulkJobId(bulkReadJobId);


            } else if (jobDetail.getStatus().equals(JobStatus.RUNNING.value)) {
                List<File> filesToBeZipped = new ArrayList<>();

                for (SubJob subJob : subJobs) {
                    Table table = DatastoreImportExportUtil.getTableByName(tables, subJob.getTable());
                    File subJobFile = DatastoreImportExportService.downloadAsset(files.get(subJob.getFile()), subJob.getFile());
                    table.getFiles().add(subJob.getFile());
                    filesToBeZipped.add(subJobFile);
                    Thread.sleep(DatastoreImportExportConstants.OPERATION_DELAY);

                }

                filesToBeZipped.add(DatastoreImportExportService.generateTableMetaJsonFromTables(tables));


                String outputFileName = DatastoreImportExportUtil.getExportZipFileName(jobDetail.getId());
                File exportZipFile = DiskFileService.createZip(filesToBeZipped, outputFileName, true);
                String outputFileId = DatastoreImportExportService.uploadAsset(exportZipFile, true);

                jobDetail.setStatus(JobStatus.SUCCESS.value);
                jobDetail.setAssetFileId(outputFileId);
                jobDetail.setAssetFileName(outputFileName);

                DatastoreImportExportService.deleteAssets(files.values().stream().toList());
                files.clear();
                subJobs.clear();

            }

        } catch (Exception exception) {

            jobDetail.setStatus(JobStatus.FAILURE.value);
            jobDetail.setMessage(exception.getMessage());

            throw exception;
        } finally {
            DatastoreImportExportService.persistJobDetail(jobDetail);
        }

    }
}
