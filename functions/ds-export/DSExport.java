import com.catalyst.Context;
import com.catalyst.basic.BasicIO;
import com.catalyst.basic.ZCFunction;
import com.zc.common.ZCProject;
import enums.ExportAction;
import handlers.DatastoreExportHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DSExport implements ZCFunction {
    private static final Logger LOGGER = Logger.getLogger(DSExport.class.getName());

    @Override
    public void runner(Context context, BasicIO basicIO) throws Exception {
        try {
            ZCProject.initProject();
            String action = String.valueOf(basicIO.getParameter("action"));

            if (action == null) {
                throw new Exception("action cannot be empty");
            }


            if (action.equals(ExportAction.START.value)) {
                String domain = basicIO.getParameter("domain").toString();

                if (domain == null) {
                    throw new Exception("domain cannot be empty");
                }

                DatastoreExportHandler.startExport(domain);
            } else if (action.equals(ExportAction.END.value)) {
                DatastoreExportHandler.endExport();
            }


        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception in DSExport", e);
            basicIO.setStatus(500);
        }
        basicIO.write("Hello From DSExport.java");
    }

}