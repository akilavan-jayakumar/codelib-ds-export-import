package enums;

public enum SubJobOperation {

    READ(1, "read"), CREATE(2, "create"), UPDATE(3, "update"), BULK_CREATE(4, "bulk-create");

    public final Integer value;
    public final String mappedValue;

    SubJobOperation(Integer value, String mappedValue) {
        this.value = value;
        this.mappedValue = mappedValue;
    }

}