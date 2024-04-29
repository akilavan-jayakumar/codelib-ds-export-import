package handlers;

import enums.CommonResponseMessage;
import exceptions.HttpException;
import org.springframework.http.HttpStatus;
import web.ResponseWrapper;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ErrorHandler {
    private static final Logger LOGGER = Logger.getLogger(ErrorHandler.class.getName());

    public static ResponseWrapper handleError(Exception exception) {
        ResponseWrapper responseWrapper = new ResponseWrapper();
        if (exception instanceof HttpException httpException) {
            responseWrapper.setMessage(httpException.getMessage());
            responseWrapper.setHttpStatus(httpException.getHttpStatus());
        } else {
            LOGGER.log(Level.SEVERE, "Exception in datastore import export ::: ", exception);
            responseWrapper.setHttpStatus(HttpStatus.INTERNAL_SERVER_ERROR);
            responseWrapper.setMessage(CommonResponseMessage.INTERNAL_SERVER_ERROR.message());
        }

        return responseWrapper;
    }
}
