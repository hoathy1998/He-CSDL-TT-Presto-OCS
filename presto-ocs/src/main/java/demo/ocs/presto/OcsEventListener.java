package demo.ocs.presto;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * EventListener (Mục 3.4 bước 4 của bài báo: Presto ghi lại lịch sử thực thi). Mỗi truy vấn hoàn tất được ghi một dòng
 * JSON gồm thời gian planning/execution/CPU, số byte/dòng, lỗi (nếu có), thời gian Local Optimizer, các toán tử đã
 * đẩy xuống và tổng thời gian theo loại toán tử Presto — đủ để phân rã chi phí (Q4) mà không cần đo bên ngoài.
 */
public final class OcsEventListener implements EventListener
{
    private final String path;

    OcsEventListener(String path) { this.path = path; }

    static final class Factory implements EventListenerFactory
    {
        @Override public String getName() { return "ocs-history"; }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new OcsEventListener(config.getOrDefault("ocs.history-file", "/results/presto/history.jsonl"));
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent e)
    {
        QueryStatistics st = e.getStatistics();
        ObjectNode o = OcsClient.JSON.createObjectNode();
        o.put("query_id", e.getMetadata().getQueryId());
        o.put("state", e.getMetadata().getQueryState());
        o.put("query", e.getMetadata().getQuery());
        o.put("create_time", e.getCreateTime().toString());
        o.put("end_time", e.getEndTime().toString());
        o.put("elapsed_ms", e.getEndTime().toEpochMilli() - e.getCreateTime().toEpochMilli());
        o.put("queued_ms", st.getQueuedTime().toMillis());
        o.put("planning_ms", st.getPlanningTime().toMillis());
        o.put("execution_ms", st.getExecutionTime().toMillis());
        o.put("cpu_ms", st.getCpuTime().toMillis());
        o.put("wall_ms", st.getWallTime().toMillis());
        o.put("total_bytes", st.getTotalBytes());
        o.put("total_rows", st.getTotalRows());
        o.put("output_rows", st.getOutputRows());
        o.put("completed_splits", st.getCompletedSplits());
        e.getFailureInfo().ifPresent(f -> o.put("failure", f.getFailureMessage().orElse(f.getErrorCode().getName())));
        OcsRuntime.OptimizerInfo opt = OcsRuntime.takeOptimizer(e.getMetadata().getQueryId());
        if (opt != null) {
            o.put("ocs_optimizer_ms", opt.nanos / 1e6);
            ArrayNode p = o.putArray("ocs_pushed");
            opt.pushed.forEach(p::add);
            o.put("ocs_substrait_ms", opt.substraitNanos / 1e6);
            o.put("ocs_substrait_plans", opt.substraitPlans);
            o.put("ocs_substrait_failures", opt.substraitFailures);
            if (opt.substraitError != null) {
                o.put("ocs_substrait_error", opt.substraitError);
            }
        }
        OcsPageSourceProvider.forgetQuery(e.getMetadata().getQueryId());
        // tổng thời gian (addInput + getOutput + finish) và số dòng ra theo loại toán tử
        Map<String, double[]> ops = new TreeMap<>();
        for (OperatorStatistics s : e.getOperatorStatistics()) {
            double[] a = ops.computeIfAbsent(s.getOperatorType(), k -> new double[3]);
            a[0] += s.getAddInputWall().toMillis() + s.getGetOutputWall().toMillis() + s.getFinishWall().toMillis();
            a[1] += s.getAddInputCpu().toMillis() + s.getGetOutputCpu().toMillis() + s.getFinishCpu().toMillis();
            a[2] += s.getOutputPositions();
        }
        ObjectNode on = o.putObject("operators");
        ops.forEach((k, a) -> {
            ObjectNode x = on.putObject(k);
            x.put("wall_ms", a[0]);
            x.put("cpu_ms", a[1]);
            x.put("output_rows", (long) a[2]);
        });
        synchronized (OcsEventListener.class) {
            try (Writer w = new OutputStreamWriter(new FileOutputStream(path, true), StandardCharsets.UTF_8)) {
                w.write(OcsClient.JSON.writeValueAsString(o));
                w.write('\n');
            }
            catch (IOException ex) {
                // không để lỗi ghi log làm hỏng truy vấn
            }
        }
    }
}
