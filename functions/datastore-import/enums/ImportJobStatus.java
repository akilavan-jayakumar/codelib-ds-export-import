package enums;

public enum ImportJobStatus {

    PENDING(1, "pending"), RUNNING(2, "running"), SUCCESS(3, "success"), FAILURE(4, "failure");

    public final Integer value;
    public final String mappedValue;

    ImportJobStatus(Integer value, String mappedValue) {
        this.value = value;
        this.mappedValue = mappedValue;
    }

}