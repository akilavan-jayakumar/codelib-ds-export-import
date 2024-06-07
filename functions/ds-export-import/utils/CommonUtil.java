package utils;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.tika.Tika;
import org.json.JSONArray;
import org.json.JSONObject;
import services.DiskFileService;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class CommonUtil {
    public static boolean isValidJsonArray(String json) {
        try {
            new JSONArray(json);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static String getFileMimeType(File file) throws Exception {
        Tika tika = new Tika();
        return tika.detect(file);
    }

    public static String getDomainFromRequest(HttpServletRequest httpServletRequest) {
        return "https://" + httpServletRequest.getHeader("host").replaceAll(":443", "");
    }

    public static List<String> parseRequestUri(String requestUri) {
        return Arrays.stream(requestUri.split("/")).filter(obj -> !obj.isBlank()).toList();
    }

    public static JSONObject parseJsonRequestBody(HttpServletRequest httpServletRequest) throws Exception {
        String body = httpServletRequest.getReader().lines().collect(Collectors.joining());
        return new JSONObject(body);
    }

    public static Map<String, Object> parseMultipartFormData(HttpServletRequest httpServletRequest) throws Exception {
        Map<String, Object> body = new HashMap<>();
        if (ServletFileUpload.isMultipartContent(httpServletRequest)) {

            DiskFileItemFactory diskFileItemFactory = new DiskFileItemFactory();
            diskFileItemFactory.setRepository(DiskFileService.getTempDirectory().toFile());

            ServletFileUpload servletFileUpload = new ServletFileUpload(diskFileItemFactory);

            List<FileItem> fileItems = servletFileUpload.parseRequest(httpServletRequest);
            StringJoiner filePrefix = new StringJoiner("-");
            filePrefix.add(String.valueOf(System.currentTimeMillis()));
            filePrefix.add(String.valueOf(Thread.currentThread().getId()));
            for (FileItem fileItem : fileItems) {
                if (!fileItem.isFormField()) {
                    String fileName = filePrefix.add(new File(fileItem.getName()).getName()).toString();
                    File file = DiskFileService.createFile(fileName);
                    fileItem.write(file);
                    body.put(fileItem.getFieldName(), file);
                } else {
                    body.put(fileItem.getFieldName(), fileItem.getString());
                }
            }

        }

        return body;
    }
}
