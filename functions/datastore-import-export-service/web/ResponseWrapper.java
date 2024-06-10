package web;


import enums.MimeType;
import org.springframework.http.HttpStatus;

import java.util.HashMap;

public class ResponseWrapper {
    private final MimeType mimeType;
    private Object data;
    private String message;
    private HttpStatus httpStatus;

    public ResponseWrapper(MimeType mimeType) {
        this.mimeType = mimeType;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public MimeType getResponseType() {
        return mimeType;
    }

    public HashMap<String, Object> getResponseMap() throws Exception {
        HashMap<String, Object> hashMap = new HashMap<>();

        if (this.data != null) {
            hashMap.put("data", data);
        }

        if (this.message != null) {
            hashMap.put("message", this.message);
        }

        if (this.httpStatus.value() >= 200 && this.httpStatus.value() < 300) {
            hashMap.put("status", "success");
        } else {
            hashMap.put("status", "failure");
        }

        return hashMap;
    }

}
