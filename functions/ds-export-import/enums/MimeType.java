package enums;

public enum MimeType {
    APPLICATION_ZIP("application/zip"), APPLICATION_JSON("application/json");

    public final String value;

    MimeType(String value) {
        this.value = value;
    }

}
