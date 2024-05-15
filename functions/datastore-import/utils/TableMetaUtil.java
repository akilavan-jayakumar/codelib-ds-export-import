package utils;

import constants.DatastoreExportImportConstants;
import pojos.ImportFileMeta;
import pojos.Table;
import services.TableMetaService;

import java.io.File;
import java.util.List;

public class TableMetaUtil {
    public static List<Table> getAllTableMetaFromFile(File file) throws Exception {
        return TableMetaService.getAllTableMetaFromFile(file);
    }

    public static Table getTableMeta(String table) throws Exception {

        ImportFileMeta tableMetaJsonFileMeta = ImportFileMetaUtil.getAllImportFileMeta().stream().filter(obj -> obj.getName().equals(DatastoreExportImportConstants.TABLE_META_JSON)).findAny().orElse(null);

        if (tableMetaJsonFileMeta == null) {
            throw new Exception("Unable to identify " + DatastoreExportImportConstants.TABLE_META_JSON + " meta");
        }

        File tableMetaFile = DatastoreImportUtil.downloadImportAsset(tableMetaJsonFileMeta.getFile_id(), tableMetaJsonFileMeta.getName());
        List<Table> tables = getAllTableMetaFromFile(tableMetaFile);
        return tables.stream().filter(obj -> obj.getName().equals(table)).findAny().orElse(null);
    }
}
