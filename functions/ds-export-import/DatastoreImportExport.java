import com.catalyst.advanced.CatalystAdvancedIOHandler;
import com.zc.common.ZCProject;
import enums.MimeType;
import handlers.ErrorHandler;
import handlers.ResponseHandler;
import restcontrollers.DatastoreImportExportController;
import services.DiskFileService;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatastoreImportExport implements CatalystAdvancedIOHandler {
    private static final Logger LOGGER = Logger.getLogger(DatastoreImportExport.class.getName());

    @Override
    public void runner(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);

        try {
            ZCProject.initProject();
            if (httpServletRequest.getMethod().equals("GET")) {
                if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+$")) {
                    responseWrapper = DatastoreImportExportController.getJobDetail(httpServletRequest);
                } else if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+/asset$")) {
                    responseWrapper = DatastoreImportExportController.getJobAsset(httpServletRequest);
                }
            } else if (httpServletRequest.getMethod().equals("POST")) {
                if (httpServletRequest.getRequestURI().matches("^/jobs/export$")) {
                    responseWrapper = DatastoreImportExportController.createDatastoreExport(httpServletRequest);
                }else  if (httpServletRequest.getRequestURI().matches("^/jobs/import$")) {
                    responseWrapper = DatastoreImportExportController.createDatastoreImport(httpServletRequest);
                } else if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+/callback$")) {
                    responseWrapper = DatastoreImportExportController.onJobCallback(httpServletRequest);
                }
            }

        } catch (Exception e) {
            responseWrapper = ErrorHandler.handleError(e);
        }

        ResponseHandler.handleResponse(httpServletResponse, responseWrapper);

        try {
            DiskFileService.flushBaseDirectory();
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "Unable to flush the base directory.", exception);
        }
    }

}