package pojos;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private String name;
    private List<String> files = new ArrayList<>();
    private List<Column> columns =  new ArrayList<>();

    public  Table(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
