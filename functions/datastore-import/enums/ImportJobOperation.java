package enums;

public enum ImportJobOperation {
    INSERT(1), UPDATE(2);
    public final Integer value;

    ImportJobOperation(Integer value) {
        this.value = value;
    }
}
