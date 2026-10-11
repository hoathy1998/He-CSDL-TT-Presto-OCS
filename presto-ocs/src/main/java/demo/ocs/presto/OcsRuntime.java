package demo.ocs.presto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Số liệu do connector tự đo trong coordinator (cùng JVM, cùng classloader với OcsEventListener):
 * thời gian Local Optimizer và các toán tử đã đẩy xuống, theo queryId. Dùng cho Q4 (phân rã chi phí).
 */
final class OcsRuntime
{
    static final class OptimizerInfo
    {
        long nanos;
        final List<String> pushed = new ArrayList<>();
        long substraitNanos;
        int substraitPlans;
        int substraitFailures;
        String substraitError;
    }

    private static final Map<String, OptimizerInfo> OPTIMIZER = new ConcurrentHashMap<>();

    private OcsRuntime() {}

    /** Một truy vấn có thể gọi optimizer nhiều lần (mỗi cây con của catalog OCS) -> cộng dồn. */
    static void recordOptimizer(String queryId, long nanos, List<String> pushed)
    {
        OptimizerInfo info = OPTIMIZER.computeIfAbsent(queryId, k -> new OptimizerInfo());
        synchronized (info) {
            info.nanos += nanos;
            info.pushed.addAll(pushed);
        }
    }

    /** R2: thời gian sinh kế hoạch Substrait (isthmus) trong Presto; failure = phải dùng SQL dự phòng. */
    static void recordSubstrait(String queryId, long nanos, boolean ok, String error)
    {
        OptimizerInfo info = OPTIMIZER.computeIfAbsent(queryId, k -> new OptimizerInfo());
        synchronized (info) {
            info.substraitNanos += nanos;
            info.substraitPlans += ok ? 1 : 0;
            info.substraitFailures += ok ? 0 : 1;
            if (error != null) {
                info.substraitError = error;
            }
        }
    }

    static OptimizerInfo takeOptimizer(String queryId)
    {
        return OPTIMIZER.remove(queryId);
    }
}
