package enums;

import java.util.Arrays;

public enum JobStatus {

    PENDING("1", "pending"), RUNNING("2", "running"), SUCCESS("3", "success"), FAILURE("4", "failure");

    public final String value;
    public final String mappedValue;

    JobStatus(String value, String mappedValue) {
        this.value = value;
        this.mappedValue = mappedValue;
    }

    public static JobStatus getJobStatusByValue(String value){
        return Arrays.stream(JobStatus.values()).filter(obj->obj.value.equals(value)).findAny().orElse(null);
    }

}