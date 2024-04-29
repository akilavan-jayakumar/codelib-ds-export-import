package web;

import org.json.simple.JSONObject;
import org.springframework.http.HttpStatus;

import java.util.HashMap;

public class ResponseWrapper {
    private Object data;
    private String message;
    private HttpStatus httpStatus;

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

    public JSONObject getResponseJson() {
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

        return new JSONObject(hashMap);
    }

}
