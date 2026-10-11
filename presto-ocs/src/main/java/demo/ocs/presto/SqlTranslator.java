package demo.ocs.presto;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Dịch RowExpression của Presto sang biểu thức SQL mà engine nhúng của OCS hiểu được
 * (tương đương bước "reconstruct SQL" trước khi sinh Substrait trong bài báo).
 * Trả Optional.empty() nếu gặp biểu thức chưa hỗ trợ -> toán tử đó không được đẩy xuống.
 */
public final class SqlTranslator
{
    private static final Set<String> FUNCTIONS = new HashSet<>(Arrays.asList(
            "abs", "sqrt", "round", "floor", "ceil", "ceiling", "ln", "exp", "power", "pow", "sin", "cos"));
    private final FunctionMetadataManager functions;

    public SqlTranslator(FunctionMetadataManager functions) { this.functions = functions; }

    public Optional<String> translate(RowExpression e, Map<VariableReferenceExpression, String> vars)
    {
        try {
            return Optional.of(tr(e, vars));
        }
        catch (UnsupportedOperationException ex) {
            return Optional.empty();
        }
    }

    private String tr(RowExpression e, Map<VariableReferenceExpression, String> vars)
    {
        if (e instanceof VariableReferenceExpression) {
            String s = vars.get(e);
            if (s == null) {
                throw new UnsupportedOperationException("biến không có trong phạm vi: " + e);
            }
            return s;
        }
        if (e instanceof ConstantExpression) {
            return constant((ConstantExpression) e);
        }
        if (e instanceof SpecialFormExpression) {
            SpecialFormExpression s = (SpecialFormExpression) e;
            List<String> a = new ArrayList<>();
            for (RowExpression x : s.getArguments()) {
                a.add(tr(x, vars));
            }
            switch (s.getForm()) {
                case AND: return "(" + String.join(" AND ", a) + ")";
                case OR: return "(" + String.join(" OR ", a) + ")";
                case IS_NULL: return "(" + a.get(0) + " IS NULL)";
                case COALESCE: return "coalesce(" + String.join(", ", a) + ")";
                case IN: return "(" + a.get(0) + " IN (" + String.join(", ", a.subList(1, a.size())) + "))";
                default: throw new UnsupportedOperationException(s.getForm().toString());
            }
        }
        if (e instanceof CallExpression) {
            CallExpression c = (CallExpression) e;
            FunctionMetadata md = functions.getFunctionMetadata(c.getFunctionHandle());
            List<String> a = new ArrayList<>();
            for (RowExpression x : c.getArguments()) {
                a.add(tr(x, vars));
            }
            Optional<OperatorType> op = md.getOperatorType();
            if (op.isPresent()) {
                switch (op.get()) {
                    case ADD: return "(" + a.get(0) + " + " + a.get(1) + ")";
                    case SUBTRACT: return "(" + a.get(0) + " - " + a.get(1) + ")";
                    case MULTIPLY: return "(" + a.get(0) + " * " + a.get(1) + ")";
                    case DIVIDE: return "(" + a.get(0) + " / " + a.get(1) + ")";
                    case MODULUS: return "(" + a.get(0) + " % " + a.get(1) + ")";
                    case NEGATION: return "(-" + a.get(0) + ")";
                    case EQUAL: return "(" + a.get(0) + " = " + a.get(1) + ")";
                    case NOT_EQUAL: return "(" + a.get(0) + " <> " + a.get(1) + ")";
                    case LESS_THAN: return "(" + a.get(0) + " < " + a.get(1) + ")";
                    case LESS_THAN_OR_EQUAL: return "(" + a.get(0) + " <= " + a.get(1) + ")";
                    case GREATER_THAN: return "(" + a.get(0) + " > " + a.get(1) + ")";
                    case GREATER_THAN_OR_EQUAL: return "(" + a.get(0) + " >= " + a.get(1) + ")";
                    case BETWEEN: return "(" + a.get(0) + " BETWEEN " + a.get(1) + " AND " + a.get(2) + ")";
                    case CAST: return "CAST(" + a.get(0) + " AS " + sqlType(c.getType()) + ")";
                    default: throw new UnsupportedOperationException(op.get().toString());
                }
            }
            String name = md.getName().getObjectName().toLowerCase();
            if (name.equals("not")) {
                return "(NOT " + a.get(0) + ")";
            }
            if (FUNCTIONS.contains(name)) {
                return name + "(" + String.join(", ", a) + ")";
            }
            throw new UnsupportedOperationException("hàm " + name);
        }
        throw new UnsupportedOperationException(e.getClass().getSimpleName());
    }

    private static String constant(ConstantExpression c)
    {
        Object v = c.getValue();
        Type t = c.getType();
        if (v == null) {
            return "NULL";
        }
        if (t instanceof BigintType || t instanceof IntegerType) {
            return v.toString();
        }
        if (t instanceof DoubleType) {
            return "CAST(" + v + " AS DOUBLE)";
        }
        if (t instanceof BooleanType) {
            return ((Boolean) v) ? "TRUE" : "FALSE";
        }
        if (t instanceof DateType) {
            return "DATE '" + LocalDate.ofEpochDay((Long) v) + "'";
        }
        if (t instanceof VarcharType) {
            return "'" + ((Slice) v).toStringUtf8().replace("'", "''") + "'";
        }
        if (t instanceof DecimalType) {     // R3: short decimal = Long (unscaled), long decimal = Slice
            DecimalType d = (DecimalType) t;
            BigInteger unscaled = v instanceof Long ? BigInteger.valueOf((Long) v) : Decimals.decodeUnscaledValue((Slice) v);
            return "CAST('" + new BigDecimal(unscaled, d.getScale()).toPlainString() + "' AS " + sqlType(t) + ")";
        }
        throw new UnsupportedOperationException("hằng số kiểu " + t);
    }

    static String sqlType(Type t)
    {
        if (t instanceof BigintType) {
            return "BIGINT";
        }
        if (t instanceof IntegerType) {
            return "INT";
        }
        if (t instanceof DoubleType) {
            return "DOUBLE";
        }
        if (t instanceof DateType) {
            return "DATE";
        }
        if (t instanceof VarcharType) {
            return "VARCHAR";
        }
        if (t instanceof DecimalType) {
            return "DECIMAL(" + ((DecimalType) t).getPrecision() + ", " + ((DecimalType) t).getScale() + ")";
        }
        throw new UnsupportedOperationException("CAST sang " + t);
    }
}
