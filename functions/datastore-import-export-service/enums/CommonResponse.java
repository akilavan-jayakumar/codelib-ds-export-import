package enums;

import org.springframework.http.HttpStatus;

public enum CommonResponse {
    JOB_NOT_FOUND(HttpStatus.NOT_FOUND,CommonResponseMessage.JOB_NOT_FOUND),
    JOB_CREATION_FORBIDDEN_DUE_TO_IMPORT(HttpStatus.FORBIDDEN,CommonResponseMessage.JOB_CREATION_FORBIDDEN_DUE_TO_IMPORT),
    JOB_CREATION_FORBIDDEN_DUE_TO_EXPORT(HttpStatus.FORBIDDEN,CommonResponseMessage.JOB_CREATION_FORBIDDEN_DUE_TO_EXPORT),
    JOB_CREATION_FORBIDDEN_DUE_TO_NO_TABLE(HttpStatus.FORBIDDEN,CommonResponseMessage.JOB_CREATION_FORBIDDEN_DUE_TO_NO_TABLE),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_FAILED_JOB(HttpStatus.FORBIDDEN,CommonResponseMessage.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_FAILED_JOB),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_PENDING_JOB(HttpStatus.FORBIDDEN,CommonResponseMessage.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_PENDING_JOB),
    GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_RUNNING_JOB(HttpStatus.FORBIDDEN,CommonResponseMessage.GET_EXPORT_ZIP_FORBIDDEN_DUE_TO_RUNNING_JOB);

    public final HttpStatus httpStatus;
    public final String message;

    CommonResponse(HttpStatus httpStatus, CommonResponseMessage commonResponseMessage) {
        this.httpStatus = httpStatus;
        this.message = commonResponseMessage.message;
    }



}
