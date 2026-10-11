package demo.ocs.presto;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * R2: sinh kế hoạch Substrait ngay trong Presto như bài báo (SQL dựng lại -> Calcite -> Substrait, qua substrait-java/isthmus).
 * Mỗi truy vấn dịch mỗi câu SQL một lần rồi dùng lại cho mọi split (khóa cache gồm queryId để Q4 đo đúng chi phí này).
 * Nếu isthmus không dịch được, trả Optional.empty() và connector gửi SQL để OCS tự dịch (đường dự phòng).
 *
 * isthmus cần jackson 2.16 và protobuf 3.25, xung đột với jackson-annotations mà Presto nạp parent-first và với protobuf
 * của Arrow Flight, nên được nạp trong classloader cô lập (thư mục substrait-lib cạnh jar plugin, cha là platform
 * classloader). Hai phía chỉ trao đổi String / byte[] qua reflection.
 */
final class SubstraitPlanner
{
    private static final String SEP = "\u0001";
    private final Map<String, Optional<String>> cache = new ConcurrentHashMap<>();
    private ClassLoader loader;
    private Object converter;
    private Method execute;

    void forget(String queryId)
    {
        cache.keySet().removeIf(k -> k.startsWith(queryId + SEP));
    }

    Optional<String> plan(String sql, List<String> columns, Map<String, String> types, String queryId)
    {
        String ddl = createTable(columns, types);
        return cache.computeIfAbsent(queryId + SEP + ddl + SEP + sql, k -> {
            long t0 = System.nanoTime();
            Optional<String> out;
            String err = null;
            try {
                out = Optional.of(Base64.getEncoder().encodeToString(toSubstrait(sql, ddl)));
            }
            catch (Exception | LinkageError e) {
                out = Optional.empty();
                Throwable c = e instanceof InvocationTargetException ? e.getCause() : e;
                StringWriter sw = new StringWriter();
                c.printStackTrace(new PrintWriter(sw));
                err = sw.toString().substring(0, Math.min(1500, sw.toString().length()));
            }
            OcsRuntime.recordSubstrait(queryId, System.nanoTime() - t0, out.isPresent(), err);
            return out;
        });
    }

    private synchronized byte[] toSubstrait(String sql, String ddl) throws Exception
    {
        if (converter == null) {
            File jar = new File(SubstraitPlanner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            File[] libs = new File(jar.getParentFile(), "substrait-lib").listFiles((d, n) -> n.endsWith(".jar"));
            if (libs == null || libs.length == 0) {
                throw new IllegalStateException("không tìm thấy substrait-lib cạnh " + jar);
            }
            URL[] urls = new URL[libs.length];
            for (int i = 0; i < libs.length; i++) {
                urls[i] = libs[i].toURI().toURL();
            }
            loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
            Class<?> c = loader.loadClass("io.substrait.isthmus.SqlToSubstrait");
            Class<?> fb = loader.loadClass("io.substrait.isthmus.FeatureBoard");
            execute = c.getMethod("execute", String.class, List.class);
            converter = withLoader(() -> {
                // giữ nguyên chữ thường của định danh (mặc định Calcite viết hoa) để khớp schema bảng ở OCS
                Class<?> casing = loader.loadClass("org.apache.calcite.avatica.util.Casing");
                Object builder = loader.loadClass("io.substrait.isthmus.ImmutableFeatureBoard").getMethod("builder").invoke(null);
                builder.getClass().getMethod("unquotedCasing", casing).invoke(builder, casingValue(casing));
                Object board = builder.getClass().getMethod("build").invoke(builder);
                return c.getConstructor(fb).newInstance(board);
            });
        }
        Object plan = withLoader(() -> execute.invoke(converter, calciteDialect(sql), Collections.singletonList(ddl)));
        return (byte[]) plan.getClass().getMethod("toByteArray").invoke(plan);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object casingValue(Class<?> casing)
    {
        return Enum.valueOf((Class<Enum>) casing, "UNCHANGED");
    }

    /**
     * SQL dựng lại dùng cú pháp DataFusion; Calcite không nhận toán tử %, nên đổi "(a % b)" (SqlTranslator luôn bọc ngoặc)
     * thành MOD(a, b). Bỏ qua % nằm trong chuỗi hằng.
     */
    static String calciteDialect(String sql)
    {
        String s = sql;
        int from = 0;
        while (true) {
            int i = s.indexOf(" % ", from);
            if (i < 0) {
                return s;
            }
            if (quotesBefore(s, i) % 2 == 1) {
                from = i + 3;
                continue;
            }
            int l = i;
            for (int depth = 0; l >= 0; l--) {
                char ch = s.charAt(l);
                if (ch == ')') {
                    depth++;
                }
                else if (ch == '(' && depth-- == 0) {
                    break;
                }
            }
            int r = i + 3;
            for (int depth = 0; r < s.length(); r++) {
                char ch = s.charAt(r);
                if (ch == '(') {
                    depth++;
                }
                else if (ch == ')' && depth-- == 0) {
                    break;
                }
            }
            if (l < 0 || r >= s.length()) {
                return s;
            }
            s = s.substring(0, l) + "MOD(" + s.substring(l + 1, i) + ", " + s.substring(i + 3, r) + ")" + s.substring(r + 1);
            from = l;
        }
    }

    private static int quotesBefore(String s, int end)
    {
        int n = 0;
        for (int k = 0; k < end; k++) {
            n += s.charAt(k) == '\'' ? 1 : 0;
        }
        return n;
    }

    private interface Call
    {
        Object run() throws Exception;
    }

    /** Calcite/isthmus dùng context classloader (ServiceLoader, sinh mã) -> đặt tạm sang classloader cô lập. */
    private Object withLoader(Call call) throws Exception
    {
        Thread t = Thread.currentThread();
        ClassLoader old = t.getContextClassLoader();
        t.setContextClassLoader(loader);
        try {
            return call.run();
        }
        finally {
            t.setContextClassLoader(old);
        }
    }

    private static String createTable(List<String> columns, Map<String, String> types)
    {
        List<String> defs = new ArrayList<>();
        for (String c : columns) {
            defs.add(c + " " + types.get(c).toUpperCase());     // bigint/integer/double/date/varchar/decimal(p,s)
        }
        return "CREATE TABLE t (" + String.join(", ", defs) + ")";
    }
}
