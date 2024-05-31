package utils;

import org.json.JSONArray;

import java.util.Arrays;
import java.util.List;

public class CommonUtil {
    public static boolean isValidJsonArray(String json) {
        try {
            new JSONArray(json);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static List<String> parseRequestUri(String requestUri) {
        return Arrays.stream(requestUri.split("/")).filter(obj -> !obj.isBlank()).toList();
    }
}
