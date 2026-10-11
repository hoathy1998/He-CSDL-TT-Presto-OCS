package demo.ocs.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.transaction.IsolationLevel;
import demo.ocs.presto.OcsHandles.OcsColumnHandle;
import demo.ocs.presto.OcsHandles.OcsTableHandle;
import demo.ocs.presto.OcsHandles.OcsTableLayoutHandle;
import demo.ocs.presto.OcsHandles.OcsTransactionHandle;
import demo.ocs.presto.OcsSplits.OcsSplit;
import demo.ocs.presto.OcsSplits.OcsSplitManager;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Điểm vào plugin: Presto nạp qua META-INF/services/com.facebook.presto.spi.Plugin. */
public final class OcsPlugin implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return Collections.singletonList(new Factory());
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return Collections.singletonList(new OcsEventListener.Factory());
    }

    static final class Factory implements ConnectorFactory
    {
        @Override public String getName() { return "ocs"; }

        @Override public ConnectorHandleResolver getHandleResolver() { return new Resolver(); }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            OcsClient client = new OcsClient(config.getOrDefault("ocs.uri", "grpc://ocs:8815"));
            // ocs.pushdown-level: none (chỉ chọn cột) | filter (như S3 Select) | all (Filter/Project/Aggregation/TopN)
            String level = config.getOrDefault("ocs.pushdown-level",
                    Boolean.parseBoolean(config.getOrDefault("ocs.pushdown-enabled", "true")) ? "all" : "none");
            boolean pushdown = !level.equals("none");
            OcsPlanOptimizer optimizer = new OcsPlanOptimizer(client, context.getFunctionMetadataManager(),
                    context.getStandardFunctionResolution(), level.equals("filter"));
            // ocs.plan-format: substrait (R2, mặc định: Presto sinh Substrait bằng isthmus) | sql (OCS tự dịch SQL)
            boolean substrait = config.getOrDefault("ocs.plan-format", "substrait").equals("substrait");
            OcsPageSourceProvider pageSources = new OcsPageSourceProvider(client, substrait ? new SubstraitPlanner() : null);
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel level, boolean readOnly)
                {
                    return OcsTransactionHandle.INSTANCE;
                }

                @Override public ConnectorMetadata getMetadata(ConnectorTransactionHandle tx) { return new OcsMetadata(client); }
                @Override public ConnectorSplitManager getSplitManager() { return new OcsSplitManager(client); }
                @Override public ConnectorPageSourceProvider getPageSourceProvider() { return pageSources; }

                @Override
                public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
                {
                    return new ConnectorPlanOptimizerProvider()
                    {
                        @Override
                        public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
                        {
                            return pushdown ? Collections.singleton(optimizer) : Collections.emptySet();
                        }

                        @Override public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers() { return Collections.emptySet(); }
                    };
                }
            };
        }
    }

    static final class Resolver implements ConnectorHandleResolver
    {
        @Override public Class<? extends ConnectorTableHandle> getTableHandleClass() { return OcsTableHandle.class; }
        @Override public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() { return OcsTableLayoutHandle.class; }
        @Override public Class<? extends ColumnHandle> getColumnHandleClass() { return OcsColumnHandle.class; }
        @Override public Class<? extends ConnectorSplit> getSplitClass() { return OcsSplit.class; }
        @Override public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() { return OcsTransactionHandle.class; }
    }
}
