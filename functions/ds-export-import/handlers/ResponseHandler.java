package handlers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import enums.ResponseType;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletResponse;

public class ResponseHandler {

    public static void handleResponse(HttpServletResponse httpServletResponse, ResponseWrapper responseWrapper) throws Exception {
        httpServletResponse.setStatus(responseWrapper.getHttpStatus().value());

        if (responseWrapper.getResponseType().equals(ResponseType.APPLICATION_JSON)) {
            ObjectMapper objectMapper = new ObjectMapper();

            httpServletResponse.setContentType(ResponseType.APPLICATION_JSON.value);
            httpServletResponse.getWriter().write(objectMapper.writeValueAsString(responseWrapper.getResponseMap()));
        }

    }
}
