import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.Parser;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import java.sql.*;

public class IgniteCreateTablesSQL {

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt = null;
        System.out.println("Initializing connection to the cluster ...");
        try {
            // Initialize connection and statement
            conn = getConnection();
            System.out.println("Connected to he cluster!");
            stmt = conn.createStatement();

            // Create and Fill the tables
            createTables(conn);
            System.out.println("Created tables!");
            fillTables(conn);
            System.out.println("Filled tables!");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }



        // Setup DSLContext
        final Settings settings = new Settings();
        settings.withRenderFormatted(true);
        settings.withStatementType(StatementType.STATIC_STATEMENT);
        DSLContext context = DSL.using(conn, settings);

        // Parse the test query
        String query = "Select b.ID, b.Name, b.Address from Ill a, Info b where a.Diagnosis = 'Asthma' and " +
                "a.PatientID = b.ID";
        Parser parser = context.parser();
        Query q = parser.parseQuery(query);

        try {
            ResultSet res = stmt.executeQuery(q.getSQL());
            while (res.next()) {
                System.out.format("ID: %d, Name: %s, Address: %s\n", res.getInt("ID"),
                        res.getString("Name"), res.getString("Address"));

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



    private static void fillTables(Connection conn) throws SQLException {
        // Fill tables
        // TODO automate table filling with randomized IDs 'n shit
        String insertStmt = "INSERT INTO ILL (PatientID, Diagnosis) VALUES (?, ?)";
        PreparedStatement prep = null;

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


        insertStmt = "INSERT INTO INFO (ID, Name, Address) VALUES (?, ?, ?)";
        prep = conn.prepareStatement(insertStmt);
        prep.setInt(1, 8457);
        prep.setString(2, "Mary Miller");
        prep.setString(3, "New Street 5, 12345 Newtown");
        prep.execute();

        prep.setInt(1, 2784);
        prep.setString(2, "Adam Smith");
        prep.setString(3, "Main Street 13, 12344 Oldtown");
        prep.execute();

        prep.setInt(1, 8765);
        prep.setString(2, "Bert Miller");
        prep.setString(3, "New Street 5, 12345 Newtown");
        prep.execute();

        prep.setInt(1, 1055);
        prep.setString(2, "Johnny Cage");
        prep.setString(3, "Highway 1, 12278 Hightown");
        prep.execute();
    }

    @Nullable
    private static void createTables(Connection conn) throws SQLException {
        //TODO clustering-based fragmentation

        // Statement
        Statement stmt = conn.createStatement();

        // DROP tables
        String dropStmt = "DROP TABLE IF EXISTS ILL; DROP TABLE IF EXISTS INFO;";
        stmt.executeUpdate(dropStmt);

        String createStmt = "CREATE TABLE ILL (PatientID INT, MeshID VARCHAR, Diagnosis VARCHAR, PRIMARY KEY " +
                "(PatientID, MeshID)) WITH \"backups=0,affinityKey=Diagnosis\"";
        stmt.executeUpdate(createStmt);

        createStmt = "CREATE TABLE INFO (ID INT PRIMARY KEY, Name VARCHAR, Address VARCHAR) WITH \"backups=0," +
                "affinityKey=ID\"";
        stmt.executeUpdate(createStmt);

    }

    @Nullable
    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800;distributedJoins=true;" +
                "collocated=true");
    }

}
