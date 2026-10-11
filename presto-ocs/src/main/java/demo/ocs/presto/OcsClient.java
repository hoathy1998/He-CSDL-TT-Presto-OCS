package demo.ocs.presto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Client Arrow Flight (gRPC) tới OCS giả lập: catalog (metastore) + gửi truy vấn đẩy xuống. */
public final class OcsClient
{
    static final ObjectMapper JSON = new ObjectMapper();
    private final FlightClient client;
    private final BufferAllocator allocator;
    private volatile JsonNode catalog;

    public OcsClient(String uri)
    {
        URI u = URI.create(uri);
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.client = FlightClient.builder(allocator, Location.forGrpcInsecure(u.getHost(), u.getPort())).build();
    }

    public BufferAllocator allocator() { return allocator; }

    public synchronized JsonNode catalog()
    {
        if (catalog == null) {
            Iterator<Result> it = client.doAction(new Action("catalog", "{}".getBytes(StandardCharsets.UTF_8)));
            try {
                catalog = JSON.readTree(it.next().getBody());
            }
            catch (Exception e) {
                throw new RuntimeException("không đọc được catalog từ OCS", e);
            }
        }
        return catalog;
    }

    /** cột -> kiểu Presto (bỏ qua các cột có kiểu chưa hỗ trợ). */
    public Map<String, String> columns(String table)
    {
        Map<String, String> out = new LinkedHashMap<>();
        JsonNode cols = catalog().get(table).get("columns");
        cols.fields().forEachRemaining(e -> {
            String t = e.getValue().get("type").asText();
            String p = t.equals("int64") ? "bigint" : t.equals("int32") ? "integer" : t.equals("double") ? "double"
                    : t.startsWith("date32") ? "date" : (t.equals("string") || t.equals("large_string")) ? "varchar"
                    : t.startsWith("decimal128(") ? t.replace("decimal128", "decimal").replace(" ", "") : null;
            if (p != null) {
                out.put(e.getKey(), p);
            }
        });
        return out;
    }

    /** Cột là hằng số trong mỗi object (cột partition, hoặc min==max theo footer) -> gom nhóm hoàn chỉnh theo object. */
    public boolean constantPerSplit(String table, String column)
    {
        JsonNode t = catalog().get(table);
        if (t.hasNonNull("partition_column") && t.get("partition_column").asText().equals(column)) {
            return true;
        }
        JsonNode c = t.get("columns").get(column);
        return c != null && c.path("constant_per_split").asBoolean(false);
    }

    /** Số dòng trung bình mỗi object (thống kê kiểu Hive Metastore). */
    public double rowsPerSplit(String table)
    {
        JsonNode t = catalog().get(table);
        return t.get("rows").asDouble() / Math.max(1, t.get("splits").size());
    }

    /** Số giá trị phân biệt (NDV) của cột trên toàn bảng; không có thống kê -> coi như mỗi dòng một giá trị. */
    public double ndv(String table, String column)
    {
        JsonNode t = catalog().get(table);
        JsonNode c = t.get("columns").get(column);
        return c != null && c.has("ndv") ? c.get("ndv").asDouble() : t.get("rows").asDouble();
    }

    public FlightStream doGet(String ticketJson)
    {
        return client.getStream(new Ticket(ticketJson.getBytes(StandardCharsets.UTF_8)));
    }
}
