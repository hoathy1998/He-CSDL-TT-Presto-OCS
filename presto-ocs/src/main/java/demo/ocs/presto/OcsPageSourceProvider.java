package demo.ocs.presto;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import demo.ocs.presto.OcsHandles.OcsColumnHandle;
import demo.ocs.presto.OcsHandles.OcsTableHandle;
import demo.ocs.presto.OcsHandles.OcsTableLayoutHandle;
import demo.ocs.presto.OcsSplits.OcsSplit;
import io.airlift.slice.Slices;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * PageSourceProvider của bài báo: với mỗi split, gửi kế hoạch (SQL dựng lại từ các toán tử đẩy xuống)
 * tới OCS qua Arrow Flight, nhận kết quả Arrow và chuyển thành Presto Page cho các toán tử residual.
 */
public final class OcsPageSourceProvider implements ConnectorPageSourceProvider
{
    private static final java.util.Set<SubstraitPlanner> PLANNERS = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final OcsClient client;
    private final SubstraitPlanner planner;    // null: gửi SQL, OCS tự dịch sang Substrait

    public OcsPageSourceProvider(OcsClient client, SubstraitPlanner planner)
    {
        this.client = client;
        this.planner = planner;
        if (planner != null) {
            PLANNERS.add(planner);
        }
    }

    static void forgetQuery(String queryId)
    {
        PLANNERS.forEach(p -> p.forget(queryId));
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle tx, ConnectorSession session, ConnectorSplit split,
            ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext)
    {
        OcsTableHandle table = ((OcsTableLayoutHandle) layout).getTable();
        List<OcsColumnHandle> cols = new ArrayList<>();
        for (ColumnHandle c : columns) {
            cols.add((OcsColumnHandle) c);
        }
        String sql;
        List<String> base;
        if (table.getPushedSql() != null) {
            sql = table.getPushedSql();
            base = table.getBaseColumns();
        }
        else {   // không có toán tử nào được đẩy xuống: chỉ chọn cột
            base = new ArrayList<>();
            StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < cols.size(); i++) {
                sb.append(i > 0 ? ", " : "").append(cols.get(i).getName());
                base.add(cols.get(i).getName());
            }
            if (cols.isEmpty()) {   // vd. count(*) không cần cột: đọc 1 cột bất kỳ
                String any = client.columns(table.getTable()).keySet().iterator().next();
                base.add(any);
                sb.append(any);
            }
            sql = sb.append(" FROM t").toString();
        }
        try {
            ObjectNode ticket = OcsClient.JSON.createObjectNode();
            ticket.put("kind", "sql");
            ticket.set("split", OcsClient.JSON.readTree(((OcsSplit) split).getSplitJson()));
            ArrayNode arr = ticket.putArray("columns");
            base.forEach(arr::add);
            ticket.put("sql", sql);
            if (planner != null) {
                Optional<String> plan = planner.plan(sql, base, client.columns(table.getTable()), session.getQueryId());
                if (plan.isPresent()) {
                    ticket.put("substrait", plan.get());
                    ArrayNode names = ticket.putArray("out_names");
                    outNames(table, cols).forEach(names::add);
                }
            }
            ticket.put("rid", UUID.randomUUID().toString());
            return new OcsPageSource(client.doGet(OcsClient.JSON.writeValueAsString(ticket)), cols);
        }
        catch (Exception e) {
            throw new RuntimeException("OCS pushdown thất bại: " + e.getMessage(), e);
        }
    }

    /** Tên cột kết quả theo thứ tự SELECT: c0..cN (đã đẩy xuống) hoặc tên cột gốc (chỉ chọn cột). */
    private static List<String> outNames(OcsTableHandle table, List<OcsColumnHandle> cols)
    {
        List<String> out = new ArrayList<>();
        if (table.getPushedSql() != null) {
            int n = table.getPushedSql().split(" AS c").length - 1;
            for (int i = 0; i < n; i++) {
                out.add("c" + i);
            }
        }
        else {
            cols.forEach(c -> out.add(c.getName()));
        }
        return out;
    }

    static final class OcsPageSource implements ConnectorPageSource
    {
        private final FlightStream stream;
        private final List<OcsColumnHandle> columns;
        private boolean finished;
        private long positions;
        private long bytes;
        private long readNanos;

        OcsPageSource(FlightStream stream, List<OcsColumnHandle> columns)
        {
            this.stream = stream;
            this.columns = columns;
        }

        @Override public long getCompletedBytes() { return bytes; }
        @Override public long getCompletedPositions() { return positions; }
        @Override public long getReadTimeNanos() { return readNanos; }
        @Override public boolean isFinished() { return finished; }
        @Override public long getSystemMemoryUsage() { return 0; }

        @Override
        public Page getNextPage()
        {
            if (finished) {
                return null;
            }
            long t0 = System.nanoTime();
            boolean has = stream.next();
            readNanos += System.nanoTime() - t0;
            if (!has) {
                finished = true;
                return null;
            }
            VectorSchemaRoot root = stream.getRoot();
            int n = root.getRowCount();
            Block[] blocks = new Block[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                OcsColumnHandle h = columns.get(c);
                FieldVector v = root.getVector(h.getName());
                if (v == null) {
                    throw new IllegalStateException("OCS không trả cột " + h.getName() + " (có: " + root.getSchema() + ")");
                }
                blocks[c] = toBlock(h.prestoType(), v, n);
                bytes += v.getBufferSize();
            }
            positions += n;
            return columns.isEmpty() ? new Page(n) : new Page(n, blocks);
        }

        private static Block toBlock(Type type, FieldVector v, int n)
        {
            BlockBuilder b = type.createBlockBuilder(null, n);
            for (int i = 0; i < n; i++) {
                if (v.isNull(i)) {
                    b.appendNull();
                }
                else if (type instanceof BigintType || type instanceof IntegerType) {
                    type.writeLong(b, ((BaseIntVector) v).getValueAsLong(i));
                }
                else if (type instanceof DoubleType) {
                    type.writeDouble(b, ((Float8Vector) v).get(i));
                }
                else if (type instanceof DateType) {
                    type.writeLong(b, ((DateDayVector) v).get(i));
                }
                else if (type instanceof VarcharType) {
                    byte[] s = v instanceof LargeVarCharVector ? ((LargeVarCharVector) v).get(i) : ((VarCharVector) v).get(i);
                    type.writeSlice(b, Slices.wrappedBuffer(s));
                }
                else if (type instanceof DecimalType) {     // R3: Arrow Decimal128 -> short/long decimal của Presto
                    DecimalType d = (DecimalType) type;
                    BigDecimal x = v instanceof DecimalVector ? ((DecimalVector) v).getObject(i)
                            : v instanceof Decimal256Vector ? ((Decimal256Vector) v).getObject(i)
                            : new BigDecimal(v.getObject(i).toString());
                    x = x.setScale(d.getScale(), RoundingMode.HALF_UP);
                    if (d.isShort()) {
                        type.writeLong(b, x.unscaledValue().longValueExact());
                    }
                    else {
                        type.writeSlice(b, Decimals.encodeUnscaledValue(x.unscaledValue()));
                    }
                }
                else {
                    throw new IllegalArgumentException("kiểu không hỗ trợ: " + type);
                }
            }
            return b.build();
        }

        @Override
        public void close()
        {
            try {
                stream.close();
            }
            catch (Exception ignored) {
                // đóng luồng Flight khi query bị hủy giữa chừng
            }
        }
    }
}
