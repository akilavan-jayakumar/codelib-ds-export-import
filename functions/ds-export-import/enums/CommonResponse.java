package enums;

import org.springframework.http.HttpStatus;

public enum CommonResponse {
    RESOURCE_NOT_FOUND(HttpStatus.NOT_FOUND,CommonResponseMessage.RESOURCE_NOT_FOUND),
    EXPORT_JOB_NOT_FOUND(HttpStatus.NOT_FOUND,CommonResponseMessage.EXPORT_JOB_NOT_FOUND),
    OPERATION_FORBIDDEN_DUE_TO_IMPORT(HttpStatus.FORBIDDEN,CommonResponseMessage.OPERATION_FORBIDDEN_DUE_TO_IMPORT),
    OPERATION_FORBIDDEN_DUE_TO_EXPORT(HttpStatus.FORBIDDEN,CommonResponseMessage.OPERATION_FORBIDDEN_DUE_TO_EXPORT),
    DATASTORE_EXPORT_FORBIDDEN(HttpStatus.FORBIDDEN, CommonResponseMessage.OPERATION_FORBIDDEN_DUE_TO_EXPORT);

    public final HttpStatus httpStatus;
    public final String message;

    CommonResponse(HttpStatus httpStatus, CommonResponseMessage commonResponseMessage) {
        this.httpStatus = httpStatus;
        this.message = commonResponseMessage.message();
    }



}
