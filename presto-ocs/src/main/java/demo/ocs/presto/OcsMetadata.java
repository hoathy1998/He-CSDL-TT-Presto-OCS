package demo.ocs.presto;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import demo.ocs.presto.OcsHandles.OcsColumnHandle;
import demo.ocs.presto.OcsHandles.OcsTableHandle;
import demo.ocs.presto.OcsHandles.OcsTableLayoutHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Metadata: schema "default", mỗi bảng logic trong catalog của OCS là một bảng Presto. */
public final class OcsMetadata implements ConnectorMetadata
{
    static final String SCHEMA = "default";
    private final OcsClient client;

    public OcsMetadata(OcsClient client) { this.client = client; }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) { return Collections.singletonList(SCHEMA); }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        List<SchemaTableName> out = new ArrayList<>();
        client.catalog().fieldNames().forEachRemaining(t -> out.add(new SchemaTableName(SCHEMA, t)));
        return out;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name)
    {
        if (!SCHEMA.equals(name.getSchemaName()) || !client.catalog().has(name.getTableName())) {
            return null;
        }
        return new OcsTableHandle(name.getTableName(), null, null, null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        // Không tự thực thi TupleDomain: để Presto giữ FilterNode, Local Optimizer sẽ quyết định đẩy xuống.
        OcsTableLayoutHandle layout = new OcsTableLayoutHandle((OcsTableHandle) table);
        return Collections.singletonList(new ConnectorTableLayoutResult(new ConnectorTableLayout(layout), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        String t = ((OcsTableHandle) table).getTable();
        List<ColumnMetadata> cols = new ArrayList<>();
        client.columns(t).forEach((n, ty) -> cols.add(new ColumnMetadata(n, OcsHandles.type(ty))));
        return new ConnectorTableMetadata(new SchemaTableName(SCHEMA, t), cols);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        Map<String, ColumnHandle> out = new LinkedHashMap<>();
        client.columns(((OcsTableHandle) table).getTable()).forEach((n, ty) -> out.put(n, new OcsColumnHandle(n, ty)));
        return out;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        OcsColumnHandle c = (OcsColumnHandle) column;
        return new ColumnMetadata(c.getName(), c.prestoType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> out = new LinkedHashMap<>();
        for (SchemaTableName t : listTables(session, (String) null)) {
            if (prefix.getTableName() == null || prefix.getTableName().equals(t.getTableName())) {
                out.put(t, getTableMetadata(session, new OcsTableHandle(t.getTableName(), null, null, null)).getColumns());
            }
        }
        return out;
    }

    static TupleDomain<ColumnHandle> all() { return TupleDomain.all(); }
}
