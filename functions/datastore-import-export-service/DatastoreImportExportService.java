import com.catalyst.advanced.CatalystAdvancedIOHandler;
import com.zc.common.ZCProject;
import enums.MimeType;
import handlers.ErrorHandler;
import handlers.ResponseHandler;
import restcontrollers.DatastoreImportExportRestController;
import services.DiskFileService;
import web.ResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatastoreImportExportService implements CatalystAdvancedIOHandler {
    private static final Logger LOGGER = Logger.getLogger(DatastoreImportExportService.class.getName());

    @Override
    public void runner(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {
        ResponseWrapper responseWrapper = new ResponseWrapper(MimeType.APPLICATION_JSON);

        try {
            ZCProject.initProject();
            if (httpServletRequest.getMethod().equals("GET")) {
                if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+$")) {
                    responseWrapper = DatastoreImportExportRestController.getJobDetail(httpServletRequest);
                } else if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+/asset$")) {
                    responseWrapper = DatastoreImportExportRestController.getJobAsset(httpServletRequest);
                }
            } else if (httpServletRequest.getMethod().equals("POST")) {
                if (httpServletRequest.getRequestURI().matches("^/jobs/export$")) {
                    responseWrapper = DatastoreImportExportRestController.createDatastoreExport(httpServletRequest);
                }else  if (httpServletRequest.getRequestURI().matches("^/jobs/import$")) {
                    responseWrapper = DatastoreImportExportRestController.createDatastoreImport(httpServletRequest);
                } else if (httpServletRequest.getRequestURI().matches("^/jobs/[0-9]+/callback$")) {
                    responseWrapper = DatastoreImportExportRestController.onJobCallback(httpServletRequest);
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