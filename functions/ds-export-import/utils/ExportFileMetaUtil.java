package utils;

import pojos.ExportFileMeta;
import services.ExportFileMetaService;

import java.io.File;
import java.util.List;

public class ExportFileMetaUtil {

    public static String uploadFile(File file) throws Exception {
        return ExportFileMetaService.uploadFile(file);
    }

    public static void createExportFileMetas(List<ExportFileMeta> exportFileMetas) throws Exception {
        ExportFileMetaService.createFileMetas(exportFileMetas);
    }
}
