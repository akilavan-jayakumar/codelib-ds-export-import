package enums;

public enum ResponseType {
    FILE("file"), APPLICATION_JSON("application/json");

    public final String value;

    ResponseType(String value) {
        this.value = value;
    }

}
