import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.Parser;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import java.sql.*;

public class IgniteCreateTables {

    public static void main(String[] args) {

        Connection conn = null;
        try {
            conn = getConnection();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        fillTables(conn);


        // Setup DSLContext
        final Settings settings = new Settings();
        settings.withRenderFormatted(true);
        settings.withStatementType(StatementType.STATIC_STATEMENT);
        DSLContext context = DSL.using(conn, settings);

        // Parse the test query
        String query = "Select * from Ill where Diagnosis = 'Asthma'";
        Parser parser = context.parser();
        Query q = parser.parseQuery(query);

        try {
            ResultSet res = stmt.executeQuery(q.getSQL());
            while (res.next()) {
                System.out.format("PatientID: %d, Diagnosis: %s\n", res.getInt("PatientID"),
                        res.getString("Diagnosis"));

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Close the connection
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private static void fillTables(Connection conn) {
        // Fill tables
        // TODO automate table filling with randomized IDs 'n shit
        String insertStmt = "INSERT INTO ILL (PatientID, Diagnosis) VALUES (?, ?)";
        PreparedStatement prep = null;
        try {
            prep = conn.prepareStatement(insertStmt);
            prep.setInt(1, 8457);
            prep.setString(2, "Cough");
            prep.execute();

            prep.setInt(1, 2784);
            prep.setString(2, "Flu");
            prep.execute();

            prep.setInt(1, 2784);
            prep.setString(2, "Asthma");
            prep.execute();

            prep.setInt(1, 2784);
            prep.setString(2, "brokenLeg");
            prep.execute();

            prep.setInt(1, 8765);
            prep.setString(2, "Asthma");
            prep.execute();

            prep.setInt(1, 1055);
            prep.setString(2, "brokenArm");
            prep.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Nullable
    private static void createTables(Connection conn) throws SQLException {
        //TODO clustering-based fragmentation

        // Statement
        Statement stmt = conn.createStatement();

        // DROP tables
        String dropStmt = "DROP TABLE IF EXISTS ILL; DROP TABLE IF EXISTS INFO; DROP TABLE IF EXISTS bnla;";
        stmt.executeUpdate(dropStmt);

        String createStmt = "CREATE TABLE ILL (PatientID INT, Diagnosis VARCHAR, NonPK TINYINT, PRIMARY KEY " +
                "(PatientID, Diagnosis)) WITH \"backups=0\"";
        stmt.executeUpdate(createStmt);

    }

    @Nullable
    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800;distributedJoins=true");
    }

}
