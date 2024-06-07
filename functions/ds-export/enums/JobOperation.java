package enums;

import java.util.Arrays;

public enum JobOperation {

    IMPORT(1,"import"),EXPORT(2,"export");

    public final Integer value;
    public final String mappedValue;

    JobOperation(Integer value, String mappedValue) {
        this.value = value;
        this.mappedValue = mappedValue;
    }


    public static JobOperation getJobOperationByValue(Integer value){
        return Arrays.stream(JobOperation.values()).filter(obj->obj.value.equals(value)).findAny().orElse(null);
    }
}
