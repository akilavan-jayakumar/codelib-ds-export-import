package pojos;

import com.zc.component.object.ZCColumn;
import com.zc.component.object.ZCTable;

import java.util.List;

public class ZCTableDetail {
    private ZCTable zcTable;
    private List<ZCColumn> zcColumns;

    public ZCTable getZcTable() {
        return zcTable;
    }

    public void setZcTable(ZCTable zcTable) {
        this.zcTable = zcTable;
    }

    public List<ZCColumn> getZcColumns() {
        return zcColumns;
    }

    public void setZcColumns(List<ZCColumn> zcColumns) {
        this.zcColumns = zcColumns;
    }
}
