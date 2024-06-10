package handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import enums.MimeType;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.nio.file.Files;

public class ResponseHandler {

    public static void handleResponse(HttpServletResponse httpServletResponse, ResponseWrapper responseWrapper) throws Exception {
        httpServletResponse.setStatus(responseWrapper.getHttpStatus().value());

        if (responseWrapper.getResponseType().equals(MimeType.APPLICATION_JSON)) {
            ObjectMapper objectMapper = new ObjectMapper();

            httpServletResponse.setContentType(MimeType.APPLICATION_JSON.value);
            httpServletResponse.getWriter().write(objectMapper.writeValueAsString(responseWrapper.getResponseMap()));
        } else if (responseWrapper.getResponseType().equals(MimeType.APPLICATION_ZIP)) {
            File file = (File) responseWrapper.getData();
            httpServletResponse.setContentType(MimeType.APPLICATION_ZIP.value);
            httpServletResponse.setContentLengthLong(file.length());
            httpServletResponse.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            Files.copy(file.toPath(), httpServletResponse.getOutputStream());
        }

    }
}
