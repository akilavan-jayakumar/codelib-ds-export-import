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
    public void runner(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper();

        try {
            ZCProject.initProject();
            if (httpServletRequest.getMethod().equals("GET")) {
                if(httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+$")){
                    responseWrapper = DatastoreExportController.getJobDetail(httpServletRequest,httpServletResponse);
                }
            } else if (httpServletRequest.getMethod().equals("POST")) {
                switch (httpServletRequest.getRequestURI()) {
                    case "/export/callback": {
                        responseWrapper = DatastoreExportController.onExportCallback(httpServletRequest,httpServletResponse);
                        break;
                    }
                    case "/jobs/export": {
                        responseWrapper = DatastoreExportController.createDatastoreExport(httpServletRequest,httpServletResponse);
                        break;
                    }
                }
            }


        } catch (Exception e) {
            responseWrapper = ErrorHandler.handleError(e);
        }

        ResponseHandler.handleResponse(httpServletResponse, responseWrapper);
    }

}