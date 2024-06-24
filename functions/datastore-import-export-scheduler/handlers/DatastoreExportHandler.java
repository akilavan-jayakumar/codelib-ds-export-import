package handlers;

import enums.CommonResponseMessage;
import enums.JobStatus;
import enums.SubJobOperation;
import pojos.JobDetail;
import pojos.JobDetailParam;
import pojos.SubJob;
import pojos.Table;
import services.CatalystDatastoreService;
import services.CatalystTableService;
import services.DatastoreImportExportService;
import utils.DatastoreImportExportUtil;

import java.io.File;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatastoreExportHandler {
    private final static Logger LOGGER = Logger.getLogger(DatastoreExportHandler.class.getName());

    public static void handle(JobDetail jobDetail, String domain) throws Exception {
        try {
            LOGGER.log(Level.SEVERE, "Test ::: Started export handle execution");

            JobDetailParam jobDetailParam = jobDetail.getParams();
            List<Table> tables = jobDetailParam.getTables();
            List<SubJob> subJobs = jobDetailParam.getSubJobs();
            HashMap<String, String> files = (HashMap<String, String>) jobDetailParam.getFiles();

            if (jobDetail.getStatus().equals(JobStatus.PENDING.value)) {
                jobDetail.setStatus(JobStatus.RUNNING.value);

                tables.addAll(DatastoreImportExportService.generateTablesFromZCTables());

                for (Table table : tables) {
                    CatalystTableService catalystTableService = new CatalystTableService(table.getName());
                    Long totalRecords = catalystTableService.getTotalRecords();

                    if (totalRecords > 0) {
                        SubJob subJob = new SubJob();
                        subJob.setId(UUID.randomUUID().toString());
                        subJob.setTable(table.getName());
                        subJob.setPage(1);
                        subJob.setStatus(JobStatus.PENDING.value);
                        subJob.setOperation(SubJobOperation.READ.value);
                        subJobs.add(subJob);
                    }
                }

                if (subJobs.isEmpty()) {
                    throw new Exception(CommonResponseMessage.NO_RECORDS_AVAILABLE_FOR_DATASTORE_EXPORT.message());
                }

                SubJob pendingJob = subJobs.get(0);
                String bulkReadJobId = CatalystDatastoreService.createBulkExport(pendingJob.getTable(), pendingJob.getPage(), DatastoreImportExportUtil.getJobCallbackUrl(domain, jobDetail.getId()), new HashMap<>());
                pendingJob.setBulkJobId(bulkReadJobId);
                pendingJob.setStatus(JobStatus.RUNNING.value);


            } else if (jobDetail.getStatus().equals(JobStatus.RUNNING.value)) {
                Map<String, String> fileNameAndIdMap = new HashMap<>();

                for (SubJob subJob : subJobs) {
                    Table table = DatastoreImportExportUtil.getTableByName(tables, subJob.getTable());
                    table.getFiles().add(subJob.getFile());

                    fileNameAndIdMap.put(subJob.getFile(), files.get(subJob.getFile()));
                }

                File tableMetaJsonFile = DatastoreImportExportService.generateTableMetaJsonFileFromTables(tables);


                String outputFileName = DatastoreImportExportUtil.getExportZipFileName(jobDetail.getId());
                File exportZipFile = DatastoreImportExportService.downloadAssetsAsZipWithExtraFiles(outputFileName, fileNameAndIdMap, Collections.singletonList(tableMetaJsonFile));

                LOGGER.info("Upload started ");
                String outputFileId = DatastoreImportExportService.uploadAsset(exportZipFile, true);
                LOGGER.info("Upload ended ::: " + outputFileId);

                jobDetail.setStatus(JobStatus.SUCCESS.value);
                jobDetail.setAssetFileId(outputFileId);
                jobDetail.setAssetFileName(outputFileName);

                DatastoreImportExportService.deleteAssets(files.values().stream().toList());

                LOGGER.log(Level.SEVERE, "Test ::: Completed  export handler");

            }

        } catch (Exception exception) {

            jobDetail.setStatus(JobStatus.FAILURE.value);
            jobDetail.setMessage(exception.getMessage());

            throw exception;
        } finally {
            DatastoreImportExportService.persistJobDetail(jobDetail);
            LOGGER.log(Level.SEVERE, "Test ::: finally for handler done");
        }

    }
}
