package enums;

public enum ColumnType {
    FOREIGN_KEY("foreign key");

    public final String value;
    ColumnType(String value) {
        this.value = value;
    }
}
