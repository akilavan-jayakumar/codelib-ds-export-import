package enums;

public enum JobAction {
    START("start"), LOAD("load"),END("end");
    public final String value;

    JobAction(String value) {
        this.value = value;
    }

}
