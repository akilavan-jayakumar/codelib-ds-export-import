package enums;

import org.springframework.http.HttpStatus;

public enum CommonResponse {
    EXPORT_JOB_NOT_FOUND(HttpStatus.NOT_FOUND,CommonResponseMessage.EXPORT_JOB_NOT_FOUND),
    DATASTORE_EXPORT_FORBIDDEN(HttpStatus.FORBIDDEN, CommonResponseMessage.DATASTORE_EXPORT_FORBIDDEN);

    private final HttpStatus httpStatus;
    private final String message;

    CommonResponse(HttpStatus httpStatus, CommonResponseMessage commonResponseMessage) {
        this.httpStatus = httpStatus;
        this.message = commonResponseMessage.message();
    }

    public HttpStatus httpStatus() {
        return httpStatus;
    }

    public String message() {
        return this.message;
    }
}
