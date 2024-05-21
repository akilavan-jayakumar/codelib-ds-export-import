import com.catalyst.Context;
import com.catalyst.basic.BasicIO;
import com.catalyst.basic.ZCFunction;
import com.zc.common.ZCProject;
import constants.DatastoreExportImportConstants;
import enums.ImportJobOperation;
import enums.ImportJobStatus;
import enums.JobAction;
import handlers.ImportHandler;
import pojos.ImportJob;
import pojos.InsertedRecordsDetails;
import pojos.Table;
import processors.ImportZipProcessor;
import utils.DatastoreImportUtil;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatastoreImport implements ZCFunction {
    private static final Logger LOGGER = Logger.getLogger(DatastoreImport.class.getName());

    @Override
    public void runner(Context context, BasicIO basicIO) throws Exception {
        try {
            ZCProject.initProject();
            String action = String.valueOf(basicIO.getParameter("action"));


            if (action == null) {
                throw new Exception("action cannot be empty");
            }

            if (action.equals(JobAction.START.value)) {
                String fileId = String.valueOf(basicIO.getParameter("fileId"));
                if (fileId == null) {
                    throw new Exception("fileId cannot be empty");
                }

                File zipFile = DatastoreImportUtil.downloadImportAsset(fileId, DatastoreExportImportConstants.IMPORT_ZIP);

                ImportZipProcessor importZipProcessor = new ImportZipProcessor(zipFile);
                ImportHandler importHandler = importZipProcessor.process();

                DatastoreImportUtil.persistImportDetails(importHandler);
                DatastoreImportUtil.runLoadImportJob(importHandler.getImportJobs().get(0).getId());


            } else if (action.equals(JobAction.LOAD.value)) {
                String jobId = String.valueOf(basicIO.getParameter("jobId"));
                if (jobId == null) {
                    throw new Exception("jobId cannot be empty");
                }

                ImportHandler importHandler = DatastoreImportUtil.getImportDetails(jobId);
                ImportJob currentImportJob = importHandler.getCurrentImportJob();

                if (currentImportJob == null) {
                    throw new Exception("Unable to identify the job with id " + jobId);
                }
                try {

                    currentImportJob.setStatus(ImportJobStatus.RUNNING.value);
                    importHandler.updateImportJob(currentImportJob);
                    DatastoreImportUtil.persistImportDetails(importHandler);

                    String oldFileId = importHandler.getFileId(currentImportJob.getFile());
                    Table table = importHandler.getTable(currentImportJob.getTable());
                    File input = DatastoreImportUtil.downloadImportAsset(oldFileId, currentImportJob.getFile());


                    if (currentImportJob.getOperation().equals(ImportJobOperation.INSERT.value)) {
                        InsertedRecordsDetails insertedRecordsDetails = DatastoreImportUtil.insertRecords(input, table);
                        String newFileId = DatastoreImportUtil.uploadImportAsset(insertedRecordsDetails.getRecords());
                        importHandler.putFileId(currentImportJob.getFile(), newFileId);
                        DatastoreImportUtil.deleteImportAsset(oldFileId);

                        if (importHandler.isParentTable(currentImportJob.getTable())) {
                            // Construction of mapping file name
                            String foreignKeyMappingFileName = DatastoreImportUtil.getForeignKeyMappingJsonFileName(currentImportJob.getTable());

                            // Checking any mapping file exists
                            if (importHandler.containsFileId(foreignKeyMappingFileName)) {
                                // Updating old mapping file contents
                                String oldMappingFileId = importHandler.getFileId(foreignKeyMappingFileName);
                                File existingMappingFile = DatastoreImportUtil.downloadImportAsset(oldMappingFileId, foreignKeyMappingFileName);

                                DatastoreImportUtil.appendMappingToExistingFile(existingMappingFile, insertedRecordsDetails.getMappings());
                                String newMappingFileId = DatastoreImportUtil.uploadImportAsset(existingMappingFile);

                                importHandler.putFileId(foreignKeyMappingFileName, newMappingFileId);
                                DatastoreImportUtil.deleteImportAsset(oldMappingFileId);
                            } else {
                                // Creating new mapping file contents
                                File mappingFile = DatastoreImportUtil.createMappingFile(foreignKeyMappingFileName, insertedRecordsDetails.getMappings());
                                String newMappingFileId = DatastoreImportUtil.uploadImportAsset(mappingFile);

                                importHandler.putFileId(foreignKeyMappingFileName, newMappingFileId);
                            }
                        }
                    } else if (currentImportJob.getOperation().equals(ImportJobOperation.UPDATE.value)) {
                        DatastoreImportUtil.updateRecords(importHandler);
                    }

                    currentImportJob.setStatus(ImportJobStatus.SUCCESS.value);
                    importHandler.updateImportJob(currentImportJob);
                    DatastoreImportUtil.persistImportDetails(importHandler);


                    if (importHandler.isPendingJobExits()) {
                        DatastoreImportUtil.runLoadImportJob(importHandler.getNextPendingJob().getId());
                    } else {
                        DatastoreImportUtil.runEndImportJob();
                    }


                } catch (Exception exception) {
                    currentImportJob.setStatus(ImportJobStatus.FAILURE.value);
                    importHandler.updateImportJob(currentImportJob);
                    DatastoreImportUtil.persistImportDetails(importHandler);
                    throw exception;
                }
            } else if (action.equals(JobAction.END.value)) {
                ImportHandler importHandler = DatastoreImportUtil.getImportDetails();
                DatastoreImportUtil.cleanImportJobsAssets(importHandler);
            }
            basicIO.setStatus(200);
            basicIO.write("Data has been imported successfully.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception in DatastoreImport", e);
            basicIO.setStatus(500);
            basicIO.write("Exception in datastore import");
        }

    }

}