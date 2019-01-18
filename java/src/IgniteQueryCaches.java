import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.sql.*;
import java.util.Arrays;

public class IgniteQueryCaches {

    private Connection conn;

    private Statement stmt;

    public IgniteQueryCaches (String jdbc) {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            this.conn = DriverManager.getConnection(jdbc);
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void queryPrintResult(String sql) {
        ResultSet res = null;
        try {
            res = this.stmt.executeQuery(sql);
            ResultSetMetaData meta = res.getMetaData();
            int columns = meta.getColumnCount();
            while (res.next()) {
                for (int i = 1; i < columns; i++) {
                    System.out.print(res.getString(i)+"\t|\t");
                }
                System.out.println(res.getString(columns));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }




    public static void main(String[] args) {

       IgniteQueryCaches test = new IgniteQueryCaches("jdbc:ignite:thin://192.168.1.1;distributedJoins=true");//\"ill_0\"");
       // Number of persons suffering from diseases from cluster0 and cluster7
        test.queryPrintResult("SELECT count(DISTINCT p.ID) FROM \"ill_0\".ILL i0, \"ill_7\".ILL i7, " +
               "\"info\".INFO p WHERE p.ID = i0.PERSONID AND i0.PERSONID = i7.PERSONID");
    }

}
