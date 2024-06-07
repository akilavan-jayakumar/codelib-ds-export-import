package pojos;

public class Column {
   private String name;
   private String type;
   private ParentTable parent;

   public Column(){}

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ParentTable getParent() {
        return parent;
    }

    public void setParent(ParentTable parent) {
        this.parent = parent;
    }

}
