package demo.ocs.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import demo.ocs.presto.OcsHandles.OcsTableLayoutHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Mỗi object (file Parquet) trong OCS là một split; worker gửi kế hoạch đẩy xuống cho từng object. */
public final class OcsSplits
{
    private OcsSplits() {}

    public static final class OcsSplit implements ConnectorSplit
    {
        private final String table;
        private final String splitJson;   // nguyên văn mục split trong catalog (file, partition_values, ...)

        @JsonCreator
        public OcsSplit(@JsonProperty("table") String table, @JsonProperty("splitJson") String splitJson)
        {
            this.table = table;
            this.splitJson = splitJson;
        }

        @JsonProperty public String getTable() { return table; }
        @JsonProperty public String getSplitJson() { return splitJson; }

        @Override public NodeSelectionStrategy getNodeSelectionStrategy() { return NodeSelectionStrategy.NO_PREFERENCE; }
        @Override public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) { return Collections.emptyList(); }
        @Override public Object getInfo() { return splitJson; }
    }

    public static final class OcsSplitManager implements ConnectorSplitManager
    {
        private final OcsClient client;

        public OcsSplitManager(OcsClient client) { this.client = client; }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle tx, ConnectorSession session,
                ConnectorTableLayoutHandle layout, SplitSchedulingContext ctx)
        {
            String table = ((OcsTableLayoutHandle) layout).getTable().getTable();
            List<OcsSplit> splits = new ArrayList<>();
            for (JsonNode s : client.catalog().get(table).get("splits")) {
                splits.add(new OcsSplit(table, s.toString()));
            }
            return new FixedSplitSource(splits);
        }
    }
}
