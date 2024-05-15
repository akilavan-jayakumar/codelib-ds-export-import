package services;

import com.zc.component.object.bulk.ZCDataStoreBulk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class CatalystDatastoreService {

    public File downloadBulkReadReport(String jobId, String folder, String fileName) throws Exception {
        File file = DiskFileService.createFile(folder, fileName);

        try (InputStream inputStream = ZCDataStoreBulk.getInstance().getBulkReadInstance().downloadBulkReadJobReport(Long.parseLong(jobId))) {
            ZipInputStream zipInputStream = new ZipInputStream(inputStream);
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    if (zipEntry.isDirectory()) {
                        throw new Exception("Downloaded report zip contains a directory");
                    }
                    int bytesRead;
                    byte[] buffer = new byte[8 * 1024];
                    while ((bytesRead = zipInputStream.read(buffer)) > 0) {
                        fileOutputStream.write(buffer, 0, bytesRead);
                    }
                }
            }

            return file;
        }
    }
}
