package enums;

public enum ExportAction {

    START("start"), END("end");
    public final String value;

    ExportAction(String value) {
        this.value = value;
    }
}
