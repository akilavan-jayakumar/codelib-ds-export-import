package exceptions;

import enums.CommonResponse;
import org.springframework.http.HttpStatus;

import java.io.Serial;

public class HttpException extends Exception {
    @Serial
    private static final long serialVersionUID = -1L;

    private final HttpStatus httpStatus;

    public HttpException(HttpStatus httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public HttpException(CommonResponse commonResponse) {
        super(commonResponse.message);
        this.httpStatus = commonResponse.httpStatus;

    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}
