import com.catalyst.advanced.CatalystAdvancedIOHandler;
import com.zc.common.ZCProject;
import handlers.ErrorHandler;
import handlers.ResponseHandler;
import restcontrollers.DatastoreExportController;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.logging.Logger;

public class DSExportImport implements CatalystAdvancedIOHandler {
    private static final Logger LOGGER = Logger.getLogger(DSExportImport.class.getName());

    @Override
    public void runner(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper();

        try {
            ZCProject.initProject();
            if (request.getMethod().equals("GET")) {
                switch (request.getRequestURI()) {
                    case "/export": {
                        responseWrapper = DatastoreExportController.startExport(request, response);
                        break;
                    }
                }
            } else if (request.getMethod().equals("POST")) {
                switch (request.getRequestURI()) {
                    case "/export/callback": {
                        responseWrapper = DatastoreExportController.onExportCallback(request,response);
                    }
                }
            }


        } catch (Exception e) {
            responseWrapper = ErrorHandler.handleError(e);
        }

        ResponseHandler.handleResponse(response, responseWrapper);
    }

}