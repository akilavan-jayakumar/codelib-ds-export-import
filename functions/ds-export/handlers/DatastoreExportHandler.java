package handlers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zc.component.cache.ZCCache;
import com.zc.component.cache.ZCSegment;
import com.zc.component.files.ZCFile;
import com.zc.component.files.ZCFolder;
import com.zc.component.object.ZCColumn;
import com.zc.component.object.ZCObject;
import com.zc.component.object.ZCRowPagedResponse;
import com.zc.component.object.ZCTable;
import com.zc.component.object.bulk.ZCBulkCallbackDetails;
import com.zc.component.object.bulk.ZCBulkQueryDetails;
import com.zc.component.object.bulk.ZCBulkReadServices;
import com.zc.component.object.bulk.ZCDataStoreBulk;
import com.zc.component.object.bulk.result.ZCBulkResult;
import constants.CatalystCacheSegments;
import constants.CatalystFilestoreFolders;
import constants.DatastoreExportImportConstants;
import constants.DatastoreExportImportUrlConstants;
import enums.JobStatus;
import pojos.*;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DatastoreExportHandler {

    public static void startExport(String domain) throws Exception {
        List<ZCTable> tables = ZCObject.getInstance().getAllTables();
        List<Job> jobs = new ArrayList<>();

        for (ZCTable table : tables) {
            ZCRowPagedResponse zcRowPagedResponse = table.getPagedRows();

            if (!zcRowPagedResponse.getRows().isEmpty()) {
                Job job = new Job();
                job.setId(UUID.randomUUID().toString());
                job.setJobId("");
                job.setPage(1);
                job.setTable(table.getName());
                if (jobs.isEmpty()) {
                    job.setStatus(JobStatus.RUNNING.value);
                } else {
                    job.setStatus(JobStatus.PENDING.value);
                }
                jobs.add(job);
            }

        }

        if (!jobs.isEmpty()) {
            Job job = jobs.get(0);

            ZCBulkCallbackDetails zcBulkCallbackDetails = ZCBulkCallbackDetails.getInstance();
            zcBulkCallbackDetails.setUrl(domain + DatastoreExportImportUrlConstants.SERVER + DatastoreExportImportUrlConstants.DATASTORE_EXPORT_IMPORT + DatastoreExportImportUrlConstants.EXPORT + DatastoreExportImportUrlConstants.CALLBACK);

            ZCBulkQueryDetails zcBulkQueryDetails = ZCBulkQueryDetails.getInstance();
            zcBulkQueryDetails.setPage(job.getPage());


            ZCBulkReadServices zcBulkReadServices = ZCDataStoreBulk.getInstance().getBulkReadInstance();
            ZCBulkResult zcBulkResult = zcBulkReadServices.createBulkReadJob(job.getTable(), zcBulkQueryDetails, zcBulkCallbackDetails);

            job.setJobId(zcBulkResult.getJobId().toString());

            ObjectMapper objectMapper = new ObjectMapper();
            String jobsJsonString = objectMapper.writeValueAsString(jobs);

            ZCSegment zcSegment = ZCCache.getInstance().getSegment(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);
            zcSegment.putCacheValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS, jobsJsonString);
            zcSegment.putCacheValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES, objectMapper.writeValueAsString(Collections.emptyList()));
        }

    }

    public static void endExport() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now());

        String tmpDir = System.getProperty("java.io.tmpdir") + File.separator + DatastoreExportImportConstants.BASE_DIR;
        String zipDir = tmpDir + File.separator + DatastoreExportImportConstants.CSV_FILES_DIR;

        Path zipPath = Paths.get(zipDir);
        Files.createDirectories(zipPath);

        ZCSegment zcSegment = ZCCache.getInstance().getSegment(CatalystCacheSegments.DS_IMPORT_EXPORT.NAME);

        List<ZCTable> zcTables = ZCObject.getInstance().getAllTables();
        List<FileMeta> fileMetas = objectMapper.readValue(zcSegment.getCacheValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES), new TypeReference<List<FileMeta>>() {
        });

        HashMap<String, String> tableIdNameMapping = new HashMap<>();
        HashMap<String, String> columnIdNameMapping = new HashMap<>();

        List<Table> tables = new ArrayList<>();

        for (ZCTable zcTable : zcTables) {
            tableIdNameMapping.put(zcTable.getTableId().toString(), zcTable.getName());

            Table table = new Table(zcTable.getName());

            List<ZCColumn> zcColumns = zcTable.getAllColumns();
            List<Column> columns = new ArrayList<>();

            for (ZCColumn zcColumn : zcColumns) {
                columnIdNameMapping.put(zcColumn.getColumnId().toString(), zcColumn.getColumnName());

                ColumnProperties columnProperties = new ColumnProperties();
                columnProperties.setMandatory(zcColumn.getIsMandatory());


                Column column = new Column(zcColumn.getColumnName(), zcColumn.getDataType());
                column.setProperties(columnProperties);

                if (zcColumn.getDataType().equals("foreign key")) {
                    ParentTable parentTable = new ParentTable();
                    parentTable.setTable(zcColumn.getParentTable());
                    parentTable.setColumn(zcColumn.getParentColumn());
                    column.setParent(parentTable);
                }

                columns.add(column);
            }
            table.setColumns(columns);
            table.setFiles(fileMetas.stream().filter(obj -> obj.getTable().equals(zcTable.getName())).map(FileMeta::getName).toList());
            tables.add(table);
        }


        for (Table table : tables) {
            for (Column column : table.getColumns()) {
                if (column.getParent() != null) {
                    String tableId = column.getParent().getTable();
                    String columnId = column.getParent().getColumn();

                    column.getParent().setTable(tableIdNameMapping.get(tableId));
                    column.getParent().setColumn(columnIdNameMapping.get(columnId));
                }
            }
        }

        ZCFolder zcFolder = ZCFile.getInstance().getFolder(CatalystFilestoreFolders.DATASTORE_EXPORT_IMPORT);

        for (FileMeta fileMeta : fileMetas) {
            Long fileId = Long.parseLong(fileMeta.getFile_id());
            try (InputStream inputStream = zcFolder.downloadFile(fileId)) {
                File file = new File(zipDir + File.separator + fileMeta.getName());
                try (OutputStream outputStream = new FileOutputStream(file)) {
                    int bytesRead;
                    byte[] buffer = new byte[1024];
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
                zcFolder.deleteFile(fileId);
            }
        }


        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(zipDir + File.separator + DatastoreExportImportConstants.TABLE_META_JSON));
        bufferedWriter.write(objectMapper.writeValueAsString(tables));
        bufferedWriter.close();

        File zipFile = new File(tmpDir + File.separator + DatastoreExportImportConstants.ZIP_FILE_PREFIX + timestamp + ".zip");
        ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile));

        Files.walkFileTree(zipPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                zipOutputStream.putNextEntry(new ZipEntry(zipPath.relativize(file).toString()));
                Files.copy(file, zipOutputStream);
                zipOutputStream.closeEntry();
                return FileVisitResult.CONTINUE;
            }
        });
        zipOutputStream.close();

        zcFolder.uploadFile(zipFile);
        zcSegment.deleteCacheValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_JOBS);
        zcSegment.deleteCacheValue(CatalystCacheSegments.DS_IMPORT_EXPORT.Keys.DS_EXPORT_FILES);

    }
}
