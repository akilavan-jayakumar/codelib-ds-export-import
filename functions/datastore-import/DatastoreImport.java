import com.catalyst.Context;
import com.catalyst.basic.BasicIO;
import com.catalyst.basic.ZCFunction;
import com.zc.common.ZCProject;
import constants.DatastoreExportImportConstants;
import enums.ImportJobOperation;
import enums.ImportJobStatus;
import enums.JobAction;
import pojos.ImportFileMeta;
import pojos.ImportJob;
import pojos.Table;
import utils.DatastoreImportUtil;
import utils.ImportFileMetaUtil;
import utils.ImportJobUtil;
import utils.TableMetaUtil;

import java.io.File;
import java.util.List;
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

                List<File> importingFiles = DatastoreImportUtil.readFilesFromZip(fileId);
                File tableMetaJson = importingFiles.stream().filter(file -> file.getName().equals(DatastoreExportImportConstants.TABLE_META_JSON)).findAny().orElse(null);

                if (tableMetaJson == null) {
                    throw new Exception("Unable to identify " + DatastoreExportImportConstants.TABLE_META_JSON);
                }

                List<Table> tables = TableMetaUtil.getAllTableMetaFromFile(tableMetaJson);
                List<ImportJob> importJobs = ImportJobUtil.convertTableMetasToImportJobs(tables);
                List<ImportFileMeta> importFileMetas = ImportFileMetaUtil.convertFilesToImportFileMetas(importingFiles);
                DatastoreImportUtil.rewriteImportJobsFiles(importJobs, importFileMetas);

                ImportFileMetaUtil.createFileMetas(importFileMetas);
                ImportJobUtil.createImportJobs(importJobs);

                DatastoreImportUtil.runLoadImportJob(importJobs.get(0).getId());


            } else if (action.equals(JobAction.LOAD.value)) {
                String jobId = String.valueOf(basicIO.getParameter("jobId"));
                if (jobId == null) {
                    throw new Exception("jobId cannot be empty");
                }

                ImportJob importJob = ImportJobUtil.getImportJobById(jobId);
                if (importJob == null) {
                    throw new Exception("Unable to identify the job with id " + jobId);
                }
                try {
                    importJob.setStatus(ImportJobStatus.RUNNING.value);
                    ImportJobUtil.updateImportJob(importJob);
                    Table table = TableMetaUtil.getTableMeta(importJob.getTable());
                    File csvFile = ImportJobUtil.downloadJobAsset(jobId);

                    if (importJob.getOperation().equals(ImportJobOperation.INSERT.value)) {
                        DatastoreImportUtil.insertRecords(csvFile, table);
                    }

                    List<ImportJob> pendingJobs = ImportJobUtil.getAllImportJobsByStatus(ImportJobStatus.PENDING);

                    if (!pendingJobs.isEmpty()) {
                        DatastoreImportUtil.runLoadImportJob(pendingJobs.get(0).getId());
                    } else {
                        DatastoreImportUtil.runEndImportJob();
                    }

                    importJob.setStatus(ImportJobStatus.SUCCESS.value);
                    ImportJobUtil.updateImportJob(importJob);
                } catch (Exception exception) {
                    importJob.setStatus(ImportJobStatus.FAILURE.value);
                    ImportJobUtil.updateImportJob(importJob);
                    throw exception;
                }
            } else if (action.equals(JobAction.END.value)) {
                DatastoreImportUtil.clearImportJobsAssets();
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