package enums;

import java.text.MessageFormat;

public enum CommonResponseMessage {

    JOB_FAILED("The job has failed"),
    TABLE_NOT_FOUND("Table ''{0}'' not found."),
    JOB_SUCCEEDED("The job has been processed successfully."),
    COLUMN_NOT_FOUND("Column ''{0}'' not found in table ''{1}''."),
    FILE_NOT_FOUND_IN_ZIP("Unable to locate ''{0}'' in the ZIP archive"),
    NO_RECORDS_AVAILABLE_FOR_DATASTORE_EXPORT("No records available for datastore export."),
    DATATYPE_MISMATCH_FOR_COLUMN("Datatype mismatch for column ''{0}'' in table ''{1}'': source''s datatype is {2}, destination''s datatype is {3}."),
    PARENT_TABLE_CONFIGURATION_MISMATCH_FOR_FOREIGN_KEY_COLUMN("Parent table configuration mismatch for foreign key column ''{0}'' in table ''{1}'': source''s parent table is {2}, destination''s parent table is {3}."),
    IMPORT_OPERATION_FORBIDDEN_DUE_TO_MANDATORY_COLUMN("Import operation forbidden because column ''{0}'' is mandatory in table ''{1}''. Kindly disable the mandatory option for column '{0}' in table '{1}' to proceed further.");


    public final String message;

    CommonResponseMessage(String message) {
        this.message = message;
    }

    public String message(String ...arguments){
        return MessageFormat.format(this.message, (Object[]) arguments);
    }
}
