package handlers;

import web.ResponseWrapper;

import javax.servlet.http.HttpServletResponse;

public class ResponseHandler {

    public static void handleResponse(HttpServletResponse httpServletResponse, ResponseWrapper responseWrapper) throws Exception{
        httpServletResponse.setStatus(responseWrapper.getHttpStatus().value());
        httpServletResponse.setContentType("application/json");
        httpServletResponse.getWriter().write(responseWrapper.getResponseJson().toJSONString());
    }
}
