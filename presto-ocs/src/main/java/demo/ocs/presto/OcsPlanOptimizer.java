package demo.ocs.presto;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import demo.ocs.presto.OcsHandles.OcsColumnHandle;
import demo.ocs.presto.OcsHandles.OcsTableHandle;
import demo.ocs.presto.OcsHandles.OcsTableLayoutHandle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Local Optimizer của Presto-OCS connector (Mục 3.4 bước 1 và Mục 4 của bài báo), cài qua
 * ConnectorPlanOptimizer: duyệt cây kế hoạch từ dưới lên, nhận diện chuỗi
 *   TableScan -> Filter -> [Project] -> [Aggregation] -> [TopN]
 * dựng lại SQL tương đương và gộp vào một TableScan đã sửa (OcsTableHandle.pushedSql).
 *
 * Quy tắc chọn toán tử (Selectivity Analyzer dùng thống kê row count / NDV của catalog OCS):
 *  - Filter: luôn đẩy xuống; vị từ không dịch được (vd. LIKE, regexp) tách ra, giữ lại làm Filter residual ở Presto.
 *  - Aggregation: chỉ đẩy khi số nhóm ước lượng mỗi object (tích NDV các khóa) <= 50% số dòng mỗi object.
 *      + Khóa GROUP BY có cột hằng theo object -> gom nhóm HOÀN CHỈNH ở OCS.
 *      + Ngược lại -> HAI PHA: OCS tính partial (sum/count/min/max; avg = sum + count), Presto tính final.
 *  - TopN: đẩy xuống từng object khi phía dưới chưa gom nhóm hoặc gom nhóm hoàn chỉnh; TopN cuối giữ ở Presto.
 *  - Project: chỉ đẩy khi phía trên có Aggregation cũng được đẩy (projection đơn lẻ không giảm byte).
 */
public final class OcsPlanOptimizer implements ConnectorPlanOptimizer
{
    private final OcsClient client;
    private final FunctionMetadataManager functions;
    private final StandardFunctionResolution resolution;
    private final SqlTranslator translator;
    private final boolean filterOnly;   // ocs.pushdown-level=filter: chỉ đẩy Filter (như S3 Select)

    public OcsPlanOptimizer(OcsClient client, FunctionMetadataManager functions, StandardFunctionResolution resolution,
            boolean filterOnly)
    {
        this.client = client;
        this.functions = functions;
        this.resolution = resolution;
        this.translator = new SqlTranslator(functions);
        this.filterOnly = filterOnly;
    }

    /** Ngữ cảnh của một lần optimize (optimizer được dùng chung giữa các truy vấn đồng thời). */
    private static final class Ctx
    {
        final PlanNodeIdAllocator ids;
        final VariableAllocator vars;
        final List<String> pushed = new ArrayList<>();

        Ctx(PlanNodeIdAllocator ids, VariableAllocator vars)
        {
            this.ids = ids;
            this.vars = vars;
        }
    }

    /** Trạng thái của một cây con có thể đẩy toàn bộ xuống OCS. */
    private static final class State
    {
        TableHandle table;
        String tableName;
        Map<VariableReferenceExpression, String> exprs = new HashMap<>();
        Set<String> baseColumns = new LinkedHashSet<>();
        List<String> where = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        boolean aggregated;
        String orderLimit;
        List<String> ops = new ArrayList<>();

        State copy()
        {
            State s = new State();
            s.table = table;
            s.tableName = tableName;
            s.exprs = new HashMap<>(exprs);
            s.baseColumns = new LinkedHashSet<>(baseColumns);
            s.where = new ArrayList<>(where);
            s.groupBy = new ArrayList<>(groupBy);
            s.aggregated = aggregated;
            s.orderLimit = orderLimit;
            s.ops = new ArrayList<>(ops);
            return s;
        }
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        long t0 = System.nanoTime();
        Ctx ctx = new Ctx(idAllocator, variableAllocator);
        PlanNode out = rewrite(maxSubplan, ctx);
        OcsRuntime.recordOptimizer(session.getQueryId(), System.nanoTime() - t0, ctx.pushed);
        return out;
    }

    private PlanNode rewrite(PlanNode node, Ctx ctx)
    {
        if (node instanceof TableScanNode) {
            return node;
        }
        Optional<State> s = analyze(node);
        if (s.isPresent() && !supportedTypes(node)) {
            s = Optional.empty();
        }
        if (s.isPresent()) {
            if (node instanceof TopNNode) {      // đẩy TopN xuống từng object, giữ TopN cuối ở Presto
                PlanNode scan = buildScan(s.get(), ((TopNNode) node).getSource().getOutputVariables(), ctx);
                return node.replaceChildren(Collections.singletonList(scan));
            }
            if (node instanceof ProjectNode && !s.get().aggregated) {   // projection đơn lẻ: chỉ đẩy phần dưới
                return node.replaceChildren(Collections.singletonList(rewrite(((ProjectNode) node).getSource(), ctx)));
            }
            return buildScan(s.get(), node.getOutputVariables(), ctx);
        }
        if (node instanceof FilterNode) {
            Optional<PlanNode> partial = partialFilter((FilterNode) node, ctx);
            if (partial.isPresent()) {
                return partial.get();
            }
        }
        if (node instanceof AggregationNode && !filterOnly) {
            Optional<PlanNode> two = twoPhase((AggregationNode) node, ctx);
            if (two.isPresent()) {
                return two.get();
            }
        }
        List<PlanNode> children = new ArrayList<>();
        for (PlanNode c : node.getSources()) {
            children.add(rewrite(c, ctx));
        }
        return node.replaceChildren(children);
    }

    /** Filter có cả vị từ dịch được và không dịch được: đẩy phần dịch được, phần còn lại thành Filter residual. */
    private Optional<PlanNode> partialFilter(FilterNode f, Ctx ctx)
    {
        Optional<State> src = analyze(f.getSource());
        if (!src.isPresent() || src.get().aggregated || src.get().orderLimit != null
                || !supportedTypes(f.getSource().getOutputVariables())) {
            return Optional.empty();
        }
        List<RowExpression> conjuncts = new ArrayList<>();
        flattenAnd(f.getPredicate(), conjuncts);
        State s = src.get().copy();
        List<RowExpression> residual = new ArrayList<>();
        for (RowExpression c : conjuncts) {
            Optional<String> t = translator.translate(c, src.get().exprs);
            if (t.isPresent()) {
                s.where.add(t.get());
            }
            else {
                residual.add(c);
            }
        }
        if (s.where.size() == src.get().where.size() || residual.isEmpty()) {
            return Optional.empty();
        }
        s.ops.add("Filter(partial)");
        PlanNode scan = buildScan(s, f.getSource().getOutputVariables(), ctx);
        RowExpression pred = residual.get(0);
        for (int i = 1; i < residual.size(); i++) {
            pred = new SpecialFormExpression(SpecialFormExpression.Form.AND, BooleanType.BOOLEAN, pred, residual.get(i));
        }
        return Optional.of(new FilterNode(Optional.empty(), ctx.ids.getNextId(), scan, pred));
    }

    private static void flattenAnd(RowExpression e, List<RowExpression> out)
    {
        if (e instanceof SpecialFormExpression && ((SpecialFormExpression) e).getForm() == SpecialFormExpression.Form.AND) {
            for (RowExpression a : ((SpecialFormExpression) e).getArguments()) {
                flattenAnd(a, out);
            }
        }
        else {
            out.add(e);
        }
    }

    private static boolean supportedTypes(PlanNode node)
    {
        PlanNode n = node instanceof TopNNode ? ((TopNNode) node).getSource() : node;
        return supportedTypes(n.getOutputVariables());
    }

    private static boolean supportedTypes(List<VariableReferenceExpression> vars)
    {
        for (VariableReferenceExpression v : vars) {
            try {
                OcsHandles.typeName(v.getType());
            }
            catch (IllegalArgumentException e) {
                return false;
            }
        }
        return true;
    }

    /** Selectivity Analyzer cho Aggregation: số nhóm ước lượng mỗi object so với số dòng mỗi object. */
    private boolean reductionWorthIt(State src, List<VariableReferenceExpression> keys)
    {
        double rowsPerSplit = client.rowsPerSplit(src.tableName);
        double groups = 1;
        for (VariableReferenceExpression k : keys) {
            String e = src.exprs.get(k);
            if (e != null && src.baseColumns.contains(e) && !client.constantPerSplit(src.tableName, e)) {
                groups *= Math.max(1, client.ndv(src.tableName, e));
            }
        }
        return Math.min(groups, rowsPerSplit) <= 0.5 * rowsPerSplit;
    }

    private Optional<State> analyze(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            TableScanNode ts = (TableScanNode) node;
            if (!(ts.getTable().getConnectorHandle() instanceof OcsTableHandle)
                    || ((OcsTableHandle) ts.getTable().getConnectorHandle()).getPushedSql() != null) {
                return Optional.empty();
            }
            State s = new State();
            s.table = ts.getTable();
            s.tableName = ((OcsTableHandle) ts.getTable().getConnectorHandle()).getTable();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> e : ts.getAssignments().entrySet()) {
                String col = ((OcsColumnHandle) e.getValue()).getName();
                s.exprs.put(e.getKey(), col);
                s.baseColumns.add(col);
            }
            return Optional.of(s);
        }
        if (node instanceof FilterNode) {
            Optional<State> src = analyze(((FilterNode) node).getSource());
            if (!src.isPresent() || src.get().aggregated || src.get().orderLimit != null) {
                return Optional.empty();
            }
            Optional<String> p = translator.translate(((FilterNode) node).getPredicate(), src.get().exprs);
            if (!p.isPresent()) {
                return Optional.empty();
            }
            State s = src.get().copy();
            s.where.add(p.get());
            s.ops.add("Filter");
            return Optional.of(s);
        }
        if (node instanceof ProjectNode) {
            Optional<State> src = analyze(((ProjectNode) node).getSource());
            if (!src.isPresent() || src.get().orderLimit != null) {
                return Optional.empty();
            }
            State s = src.get().copy();
            Map<VariableReferenceExpression, String> out = new HashMap<>();
            boolean nonIdentity = false;
            for (Map.Entry<VariableReferenceExpression, RowExpression> e : ((ProjectNode) node).getAssignments().getMap().entrySet()) {
                RowExpression ex = e.getValue();
                if (src.get().aggregated && !(ex instanceof VariableReferenceExpression)) {
                    return Optional.empty();       // sau gom nhóm chỉ chấp nhận projection đổi tên
                }
                Optional<String> t = translator.translate(ex, src.get().exprs);
                if (!t.isPresent()) {
                    return Optional.empty();
                }
                nonIdentity |= !(ex instanceof VariableReferenceExpression);
                out.put(e.getKey(), t.get());
            }
            s.exprs = out;
            if (nonIdentity) {
                s.ops.add("Project");
            }
            return Optional.of(s);
        }
        if (filterOnly && (node instanceof AggregationNode || node instanceof TopNNode)) {
            return Optional.empty();
        }
        if (node instanceof AggregationNode) {
            AggregationNode a = (AggregationNode) node;
            if (!simpleSingleStep(a)) {
                return Optional.empty();
            }
            Optional<State> src = analyze(a.getSource());
            if (!src.isPresent() || src.get().aggregated || src.get().orderLimit != null) {
                return Optional.empty();
            }
            State s = src.get().copy();
            boolean complete = false;
            Map<VariableReferenceExpression, String> out = new HashMap<>();
            for (VariableReferenceExpression k : a.getGroupingKeys()) {
                String expr = src.get().exprs.get(k);
                if (expr == null) {
                    return Optional.empty();
                }
                complete |= src.get().baseColumns.contains(expr) && client.constantPerSplit(s.tableName, expr);
                s.groupBy.add(expr);
                out.put(k, expr);
            }
            if (!complete || !reductionWorthIt(src.get(), a.getGroupingKeys())) {
                return Optional.empty();       // nhóm trải nhiều object -> xử lý ở twoPhase()
            }
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> e : a.getAggregations().entrySet()) {
                AggregationNode.Aggregation g = e.getValue();
                String fn = aggName(g);
                if (fn == null || (fn.equals("avg") && !(g.getCall().getType() instanceof DoubleType))) {
                    return Optional.empty();
                }
                if (g.getArguments().isEmpty()) {
                    out.put(e.getKey(), fn + "(*)");
                    continue;
                }
                Optional<String> arg = translator.translate(g.getArguments().get(0), src.get().exprs);
                if (!arg.isPresent()) {
                    return Optional.empty();
                }
                out.put(e.getKey(), fn + "(" + arg.get() + ")");
            }
            s.exprs = out;
            s.aggregated = true;
            s.ops.add("Aggregation(COMPLETE)");
            return Optional.of(s);
        }
        if (node instanceof TopNNode) {
            TopNNode t = (TopNNode) node;
            Optional<State> src = analyze(t.getSource());
            if (!src.isPresent() || src.get().orderLimit != null) {
                return Optional.empty();
            }
            List<String> order = new ArrayList<>();
            for (VariableReferenceExpression v : t.getOrderingScheme().getOrderByVariables()) {
                String e = src.get().exprs.get(v);
                if (e == null) {
                    return Optional.empty();
                }
                SortOrder so = t.getOrderingScheme().getOrdering(v);
                order.add(e + (so.isAscending() ? " ASC" : " DESC") + (so.isNullsFirst() ? " NULLS FIRST" : " NULLS LAST"));
            }
            State s = src.get().copy();
            s.orderLimit = " ORDER BY " + String.join(", ", order) + " LIMIT " + t.getCount();
            s.ops.add("TopN(per object)");
            return Optional.of(s);
        }
        return Optional.empty();
    }

    private static boolean simpleSingleStep(AggregationNode a)
    {
        return a.getStep() == AggregationNode.Step.SINGLE && !a.getHashVariable().isPresent()
                && !a.getGroupIdVariable().isPresent() && a.getGroupingSetCount() == 1 && !a.getGroupingKeys().isEmpty();
    }

    /** Tên hàm gom nhóm hỗ trợ (sum/min/max/avg/count), hoặc null nếu không đẩy được. */
    private String aggName(AggregationNode.Aggregation g)
    {
        if (g.isDistinct() || g.getFilter().isPresent() || g.getOrderBy().isPresent() || g.getMask().isPresent()) {
            return null;
        }
        String fn = functions.getFunctionMetadata(g.getFunctionHandle()).getName().getObjectName().toLowerCase();
        return Arrays.asList("sum", "min", "max", "avg", "count").contains(fn) ? fn : null;
    }

    /**
     * Gom nhóm HAI PHA khi nhóm trải qua nhiều object: OCS tính partial theo từng object, Presto tính final.
     *   sum(x) -> sum(sum_i)   min/max -> min/max   count -> sum(count_i)   avg(x) -> sum(sum_i) / sum(cnt_i)
     */
    private Optional<PlanNode> twoPhase(AggregationNode a, Ctx ctx)
    {
        if (!simpleSingleStep(a)) {
            return Optional.empty();
        }
        Optional<State> src = analyze(a.getSource());
        if (!src.isPresent() || src.get().aggregated || src.get().orderLimit != null
                || !reductionWorthIt(src.get(), a.getGroupingKeys())) {
            return Optional.empty();
        }
        State s = src.get().copy();
        List<VariableReferenceExpression> outs = new ArrayList<>();
        Map<VariableReferenceExpression, String> exprs = new HashMap<>();
        for (VariableReferenceExpression k : a.getGroupingKeys()) {
            String e = src.get().exprs.get(k);
            if (e == null) {
                return Optional.empty();
            }
            s.groupBy.add(e);
            exprs.put(k, e);
            outs.add(k);
        }
        Map<VariableReferenceExpression, AggregationNode.Aggregation> finals = new LinkedHashMap<>();
        Map<VariableReferenceExpression, RowExpression> avgDiv = new LinkedHashMap<>();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> e : a.getAggregations().entrySet()) {
            AggregationNode.Aggregation g = e.getValue();
            String fn = aggName(g);
            if (fn == null) {
                return Optional.empty();
            }
            Type outType = g.getCall().getType();
            String arg = "*";
            if (!g.getArguments().isEmpty()) {
                Optional<String> t = translator.translate(g.getArguments().get(0), src.get().exprs);
                if (!t.isPresent()) {
                    return Optional.empty();
                }
                arg = t.get();
            }
            switch (fn) {
                case "sum":
                case "min":
                case "max": {
                    VariableReferenceExpression p = ctx.vars.newVariable("p_" + fn, outType);
                    exprs.put(p, fn + "(" + arg + ")");
                    outs.add(p);
                    FunctionHandle h = fn.equals("sum") ? lookup("sum", outType) : g.getFunctionHandle();
                    if (h == null) {
                        return Optional.empty();
                    }
                    finals.put(e.getKey(), agg(fn, h, outType, p));
                    break;
                }
                case "count": {
                    VariableReferenceExpression p = ctx.vars.newVariable("p_count", BigintType.BIGINT);
                    exprs.put(p, "count(" + arg + ")");
                    outs.add(p);
                    FunctionHandle h = lookup("sum", BigintType.BIGINT);
                    if (h == null) {
                        return Optional.empty();
                    }
                    finals.put(e.getKey(), agg("sum", h, BigintType.BIGINT, p));
                    break;
                }
                case "avg": {
                    if (!(g.getArguments().get(0).getType() instanceof DoubleType) || !(outType instanceof DoubleType)) {
                        return Optional.empty();    // avg trên decimal: ngữ nghĩa làm tròn khác nhau -> không đẩy
                    }
                    VariableReferenceExpression ps = ctx.vars.newVariable("p_sum", DoubleType.DOUBLE);
                    VariableReferenceExpression pc = ctx.vars.newVariable("p_cnt", DoubleType.DOUBLE);
                    exprs.put(ps, "sum(" + arg + ")");
                    exprs.put(pc, "CAST(count(" + arg + ") AS DOUBLE)");
                    outs.add(ps);
                    outs.add(pc);
                    FunctionHandle h = lookup("sum", DoubleType.DOUBLE);
                    if (h == null) {
                        return Optional.empty();
                    }
                    VariableReferenceExpression fs = ctx.vars.newVariable("f_sum", DoubleType.DOUBLE);
                    VariableReferenceExpression fc = ctx.vars.newVariable("f_cnt", DoubleType.DOUBLE);
                    finals.put(fs, agg("sum", h, DoubleType.DOUBLE, ps));
                    finals.put(fc, agg("sum", h, DoubleType.DOUBLE, pc));
                    FunctionHandle div = resolution.arithmeticFunction(OperatorType.DIVIDE, DoubleType.DOUBLE, DoubleType.DOUBLE);
                    avgDiv.put(e.getKey(), new CallExpression("divide", div, DoubleType.DOUBLE, Arrays.asList(fs, fc)));
                    break;
                }
                default:
                    return Optional.empty();
            }
        }
        if (!supportedTypes(outs)) {
            return Optional.empty();
        }
        s.exprs = exprs;
        s.aggregated = true;
        s.ops.add("Aggregation(PARTIAL)");
        PlanNode scan = buildScan(s, outs, ctx);
        AggregationNode fin = new AggregationNode(Optional.empty(), ctx.ids.getNextId(), scan, finals, a.getGroupingSets(),
                Collections.emptyList(), AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty(), Optional.empty());
        if (avgDiv.isEmpty()) {
            return Optional.of(fin);
        }
        Map<VariableReferenceExpression, RowExpression> assign = new LinkedHashMap<>();
        for (VariableReferenceExpression v : a.getOutputVariables()) {
            assign.put(v, avgDiv.containsKey(v) ? avgDiv.get(v) : v);
        }
        return Optional.of(new ProjectNode(Optional.empty(), ctx.ids.getNextId(), fin, Assignments.copyOf(assign),
                ProjectNode.Locality.LOCAL));
    }

    private static AggregationNode.Aggregation agg(String name, FunctionHandle h, Type type, VariableReferenceExpression arg)
    {
        return new AggregationNode.Aggregation(new CallExpression(name, h, type, Collections.singletonList(arg)),
                Optional.empty(), Optional.empty(), false, Optional.empty());
    }

    /**
     * Tra FunctionHandle của hàm gom nhóm (vd. sum(bigint)). SPI 0.286 không có API công khai cho việc này
     * (StandardFunctionResolution chỉ có count/min/max), nên gọi FunctionAndTypeManager.lookupFunction qua reflection.
     */
    private FunctionHandle lookup(String name, Type... types)
    {
        try {
            Object mgr = functions;
            Class<?> tsp = Class.forName("com.facebook.presto.sql.analyzer.TypeSignatureProvider", true,
                    mgr.getClass().getClassLoader());
            Object list = tsp.getMethod("fromTypes", List.class).invoke(null, Arrays.asList(types));
            return (FunctionHandle) mgr.getClass().getMethod("lookupFunction", String.class, List.class)
                    .invoke(mgr, name, list);
        }
        catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private PlanNode buildScan(State s, List<VariableReferenceExpression> outputs, Ctx ctx)
    {
        StringBuilder sql = new StringBuilder("SELECT ");
        Map<VariableReferenceExpression, ColumnHandle> assignments = new LinkedHashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            VariableReferenceExpression v = outputs.get(i);
            String alias = "c" + i;
            sql.append(i > 0 ? ", " : "").append(s.exprs.get(v)).append(" AS ").append(alias);
            assignments.put(v, new OcsColumnHandle(alias, OcsHandles.typeName(v.getType())));
        }
        sql.append(" FROM t");
        if (!s.where.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", s.where));
        }
        if (!s.groupBy.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", s.groupBy));
        }
        if (s.orderLimit != null) {
            sql.append(s.orderLimit);
        }
        String ops = String.join(" -> ", s.ops);
        ctx.pushed.add(s.tableName + ": " + ops);
        OcsTableHandle handle = new OcsTableHandle(s.tableName, sql.toString(), new ArrayList<>(s.baseColumns), ops);
        TableHandle th = new TableHandle(s.table.getConnectorId(), handle, s.table.getTransaction(),
                Optional.of(new OcsTableLayoutHandle(handle)));
        return new TableScanNode(Optional.empty(), ctx.ids.getNextId(), th, outputs, assignments, TupleDomain.all(), TupleDomain.all());
    }
}
