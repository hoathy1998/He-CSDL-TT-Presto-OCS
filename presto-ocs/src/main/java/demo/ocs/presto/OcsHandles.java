package demo.ocs.presto;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/** Các handle của connector (phải tuần tự hóa JSON được vì coordinator gửi chúng cho worker). */
public final class OcsHandles
{
    private OcsHandles() {}

    /** Kiểu dữ liệu: lưu dạng chuỗi để tuần tự hóa, đổi sang Presto Type khi cần. */
    public static Type type(String name)
    {
        switch (name) {
            case "bigint": return BigintType.BIGINT;
            case "integer": return IntegerType.INTEGER;
            case "double": return DoubleType.DOUBLE;
            case "date": return DateType.DATE;
            case "varchar": return VarcharType.VARCHAR;
            default:
                if (name.startsWith("decimal(")) {     // R3: decimal(p,s)
                    String[] ps = name.substring(8, name.length() - 1).split(",");
                    return DecimalType.createDecimalType(Integer.parseInt(ps[0].trim()), Integer.parseInt(ps[1].trim()));
                }
                throw new IllegalArgumentException("kiểu không hỗ trợ: " + name);
        }
    }

    public static String typeName(Type t)
    {
        if (t instanceof BigintType) {
            return "bigint";
        }
        if (t instanceof IntegerType) {
            return "integer";
        }
        if (t instanceof DoubleType) {
            return "double";
        }
        if (t instanceof DateType) {
            return "date";
        }
        if (t instanceof VarcharType) {
            return "varchar";
        }
        if (t instanceof DecimalType) {
            return "decimal(" + ((DecimalType) t).getPrecision() + "," + ((DecimalType) t).getScale() + ")";
        }
        throw new IllegalArgumentException("kiểu không hỗ trợ: " + t);
    }

    public enum OcsTransactionHandle implements ConnectorTransactionHandle { INSTANCE }

    public static final class OcsColumnHandle implements ColumnHandle
    {
        private final String name;
        private final String type;

        @JsonCreator
        public OcsColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") String type)
        {
            this.name = name;
            this.type = type;
        }

        @JsonProperty public String getName() { return name; }
        @JsonProperty public String getType() { return type; }
        public Type prestoType() { return OcsHandles.type(type); }

        @Override public boolean equals(Object o)
        {
            return o instanceof OcsColumnHandle && ((OcsColumnHandle) o).name.equals(name) && ((OcsColumnHandle) o).type.equals(type);
        }
        @Override public int hashCode() { return Objects.hash(name, type); }
        @Override public String toString() { return name + ":" + type; }
    }

    /**
     * Bảng của OCS. Khi Local Optimizer đã gộp toán tử vào TableScan, pushedSql chứa câu SQL sẽ được OCS
     * thực thi trên TỪNG object (bảng "t"), baseColumns là các cột vật lý cần đọc, pushedOps mô tả để EXPLAIN.
     */
    public static final class OcsTableHandle implements ConnectorTableHandle
    {
        private final String table;
        private final String pushedSql;
        private final List<String> baseColumns;
        private final String pushedOps;

        @JsonCreator
        public OcsTableHandle(@JsonProperty("table") String table, @JsonProperty("pushedSql") String pushedSql,
                @JsonProperty("baseColumns") List<String> baseColumns, @JsonProperty("pushedOps") String pushedOps)
        {
            this.table = table;
            this.pushedSql = pushedSql;
            this.baseColumns = baseColumns;
            this.pushedOps = pushedOps;
        }

        @JsonProperty public String getTable() { return table; }
        @JsonProperty public String getPushedSql() { return pushedSql; }
        @JsonProperty public List<String> getBaseColumns() { return baseColumns; }
        @JsonProperty public String getPushedOps() { return pushedOps; }

        @Override public boolean equals(Object o)
        {
            if (!(o instanceof OcsTableHandle)) {
                return false;
            }
            OcsTableHandle h = (OcsTableHandle) o;
            return table.equals(h.table) && Objects.equals(pushedSql, h.pushedSql);
        }
        @Override public int hashCode() { return Objects.hash(table, pushedSql); }
        @Override public String toString()
        {
            return pushedSql == null ? "ocs:" + table : "ocs:" + table + " pushdown=[" + pushedOps + "] sql=[" + pushedSql + "]";
        }
    }

    public static final class OcsTableLayoutHandle implements ConnectorTableLayoutHandle
    {
        private final OcsTableHandle table;

        @JsonCreator
        public OcsTableLayoutHandle(@JsonProperty("table") OcsTableHandle table) { this.table = table; }

        @JsonProperty public OcsTableHandle getTable() { return table; }
        @Override public boolean equals(Object o) { return o instanceof OcsTableLayoutHandle && ((OcsTableLayoutHandle) o).table.equals(table); }
        @Override public int hashCode() { return table.hashCode(); }
        @Override public String toString() { return table.toString(); }
    }
}
