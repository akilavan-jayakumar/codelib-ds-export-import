package enums;

public enum CommonResponseMessage {
    COMMON_PROCESSING_MESSAGE("Request has been processed successfully."),
    JOB_NOT_FOUND("We couldn't find the requested job on the server."),
    INTERNAL_SERVER_ERROR("We're unable to process your request. Kindly try after sometime"),
    JOB_CREATION_FORBIDDEN_DUE_TO_NO_TABLE("You're not allowed to perform this operation because there is no table available"),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_PENDING_JOB("You are not allowed to perform this operation because this export job is in a pending state."),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_RUNNING_JOB("You are not allowed to perform this operation because this export job is in a running state."),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_FAILED_JOB("You are not allowed to perform this operation because this export job has failed."),
    JOB_CREATION_FORBIDDEN_DUE_TO_IMPORT("You are not allowed to perform this operation because the datastore is currently being imported."),
    JOB_CREATION_FORBIDDEN_DUE_TO_EXPORT("You are not allowed to perform this operation because the datastore is currently being exported.");

    public final String message;

    CommonResponseMessage(String message) {
        this.message = message;
    }
}
