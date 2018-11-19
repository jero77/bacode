import java.sql.*;
import java.util.Map;

import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.DefaultPreparedStatement;

import static org.jooq.impl.DSL.*;

public class test {
    public static void main(String[] args) {

        System.out.println(System.getProperty("java.class.path"));

        // Init connection
        Connection conn = null;

        // Setup DSLContext
        final Settings settings = new Settings();
        settings.withRenderFormatted(true);
//        settings.withParamType(ParamType.INLINED);
//        settings.withStatementType(StatementType.PREPARED_STATEMENT);
//        settings.withParamType(ParamType.INDEXED);
        settings.withStatementType(StatementType.STATIC_STATEMENT);
        DSLContext context = DSL.using(SQLDialect.SQLITE, settings);

        // Test query to String
        final SelectQuery<Record> query = context.select(asterisk())
                .from("Ill")
                .where("Diagnosis = 'Bronchitis'")
                .getQuery();
        String sql = query.getSQL();
        System.out.println("SQL:\n" + sql);
        System.out.println("getBindValues(): " + query.getBindValues());    // empty, but maybe of use later
        System.out.println("getParams(): " + query.getParams());            //      -       "       -

        // Parse the test query
        Parser parser = context.parser();
        Query q = parser.parseQuery(sql);
        System.out.println("\nSQL: " + q.getSQL());

        // Modify/Inspect/Analyze the parsed query q
        System.out.println("isExecutable: " + q.isExecutable());
        System.out.println("getBindValues(): " + q.getBindValues());    // empty, but maybe of use later
        System.out.println("getParams(): " + q.getParams());            // NOT EMPTY, contains 1='Bronchitis'

        Map<String, Param<?>> map = context.extractParams(q);           // equals getParams()
        System.out.println("\nMap: " + map);
        for (Param<?> param : map.values()) {
            System.out.println("Name: " + param.getName() + ", Value: " + param.getValue());
            System.out.println("ParamName: " + param.getParamName() + ", isInline: " + param.isInline());
            System.out.println("ParamMode: " + param.getParamMode() + ", ParamType: " + param.getParamType());
            System.out.println("QualifiedName(): " + param.getQualifiedName());
            System.out.println("getBinding(): " + param.getBinding());
        }

        // Param by name
        Param<Object> param = (Param<Object>) q.getParam("1");
        System.out.println("\n" + param.getParamName());        // would be great if this was the name of the attribute
        System.out.println(q.bind("1", "Cough"));        // query is modifiable (can be bound)
        param.setValue("Cough");                                // Param value is modifiable
        System.out.println(param);
        System.out.println(q.getSQL());
        System.out.println(q.getParams());
    }
}
