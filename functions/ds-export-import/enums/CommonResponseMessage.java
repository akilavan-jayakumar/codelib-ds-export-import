package enums;

public enum CommonResponseMessage {
    EXPORT_JOB_NOT_FOUND("We couldn't find the requested job on the server."),
    DATASTORE_EXPORT_SCHEDULED("Datastore export has been scheduled successfully."),
    INTERNAL_SERVER_ERROR("We're unable to process your request. Kindly try after sometime"),
    DATASTORE_EXPORT_FORBIDDEN("You're not allowed to perform this operation as datastore is currently being exported.");

    private final String message;

    CommonResponseMessage(String message) {
        this.message = message;
    }

    public String message() {
        return message;
    }
}
