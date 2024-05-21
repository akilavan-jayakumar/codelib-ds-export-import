package processors;

import constants.DatastoreExportImportConstants;
import handlers.ImportHandler;
import pojos.ImportJob;
import pojos.Table;
import utils.DatastoreImportUtil;
import utils.DiskFileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ImportZipProcessor {
    private final File zip;

    public ImportZipProcessor(File zip) {
        this.zip = zip;
    }

    public ImportHandler process() throws Exception {
        List<Table> tables = new ArrayList<>();
        HashMap<String, String> files = new HashMap<>();
        List<File> unzippedFiles = DiskFileUtil.unzip(zip, true);

        for (File file : unzippedFiles) {
            if (file.getName().equals(DatastoreExportImportConstants.TABLE_META_JSON)) {
                tables = DatastoreImportUtil.getTablesFromFile(file);
            }

            files.put(file.getName(), DatastoreImportUtil.uploadImportAsset(file));
            Thread.sleep(DatastoreExportImportConstants.OPERATION_DELAY);
        }

        List<ImportJob> importJobs = DatastoreImportUtil.createImportJobsFromTables(tables);

        return new ImportHandler(tables, files,importJobs);
    }
}
