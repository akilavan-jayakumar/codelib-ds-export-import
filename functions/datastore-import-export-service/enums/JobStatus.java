package enums;

import java.util.Arrays;

public enum JobStatus {

    PENDING(1, "pending"), RUNNING(2, "running"), SUCCESS(3, "success"), FAILURE(4, "failure");

    public final Integer value;
    public final String mappedValue;

    JobStatus(Integer value, String mappedValue) {
        this.value = value;
        this.mappedValue = mappedValue;
    }

    public static JobStatus getJobStatusByValue(Integer value){
        return Arrays.stream(JobStatus.values()).filter(obj->obj.value.equals(value)).findAny().orElse(null);
    }

}