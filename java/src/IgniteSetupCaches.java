import org.apache.commons.lang3.ArrayUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.Cache.Entry;
import java.io.File;
import java.sql.*;
import java.util.*;

public class IgniteSetupCaches {

    /**
     * Affinity Function with clustering and partition mappings
     */
    private MyAffinityFunction affinityFunction;

    /**
     * Cache config for Ill-Cache
     */
    private CacheConfiguration<IllKey, Ill> cacheConfigIll;

    /**
     * Cache config for Info-cache
     */
    private CacheConfiguration<InfoKey, Info> cacheConfigInfo;

    /**
     * Array for cache configs of Info fragment caches
     */
    private CacheConfiguration<IllKey, Ill>[] fragmentConfigs;

    /**
     * Client node
     */
    private Ignite client;

    /**
     * Ignite configuration for the client
     */
    private IgniteConfiguration clientConfig;



// ################################ Constructors ###############################################

    /**
     * Constructor
     * @param affinityFunction Affinity Function for clustering-based fragmentation
     * @param addresses Addresses of the ignite nodes for DiscoverySpi
     */
    public IgniteSetupCaches(MyAffinityFunction affinityFunction, Collection<String> addresses) {
        this.affinityFunction = affinityFunction;

        // Cache configurations for base tables
        this.cacheConfigIll = new CacheConfiguration<IllKey, Ill>();
        this.cacheConfigIll.setName("ill")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(IllKey.class, Ill.class)
                .setAffinity(this.affinityFunction);

        this.cacheConfigInfo = new CacheConfiguration<InfoKey, Info>();
        this.cacheConfigInfo.setName("info")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(InfoKey.class, Info.class)
                .setAffinity(this.affinityFunction);

        // Cache configs for fragments
        this.fragmentConfigs = this.makeFragmentCacheConfigs();

        // Config for the client
            // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(addresses);
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        CacheConfiguration[] cacheConfigurations = ArrayUtils.addAll(this.fragmentConfigs, this.cacheConfigInfo);
        this.clientConfig = new IgniteConfiguration();
        this.clientConfig.setClientMode(true)
                .setDiscoverySpi(spi)
                .setCacheConfiguration(cacheConfigurations);
//                .setPeerClassLoadingEnabled(true);

        // Connect this client to the cluster & get or create the caches
        this.client = Ignition.start(this.clientConfig);
        System.out.println("Connected!");

        client.getOrCreateCaches(Arrays.asList(cacheConfigurations));
        System.out.println("Got/created all the caches: " + client.cacheNames());
    }



// ################################ Getter & Setter ############################################


    public CacheConfiguration<IllKey, Ill> getCacheConfigIll() {
        return cacheConfigIll;
    }

    public CacheConfiguration<InfoKey, Info> getCacheConfigInfo() {
        return cacheConfigInfo;
    }

    public CacheConfiguration<IllKey, Ill>[] getFragmentConfigs() {
        return fragmentConfigs;
    }


// ################################ Private Methods ############################################

    /**
     * Get the cache configurations for the caches of the clustering-based fragments of the Ill-Cache.
     * @return Cache configs (index = fragment id --> 'ill_id')
     */
    private CacheConfiguration<IllKey, Ill>[] makeFragmentCacheConfigs() {

        // Get clustering from affinity function
        ArrayList<Cluster<String>> clustering = this.affinityFunction.getClusters();

        // Create configs
        CacheConfiguration<IllKey, Ill>[] fragmentConfigs = new CacheConfiguration[clustering.size()];
        for (int id = 0; id < clustering.size(); id++) {
            Cluster cluster = clustering.get(id);
            CacheConfiguration<IllKey, Ill> cacheConfigIllFrag = new CacheConfiguration<IllKey, Ill>();
            cacheConfigIllFrag.setName("ill_"+id)
                    .setBackups(0)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setIndexedTypes(IllKey.class, Ill.class)
                    .setAffinity(this.affinityFunction);
            fragmentConfigs[id] = cacheConfigIllFrag;
        }
        return fragmentConfigs;
    }


// ################################ Public Methods ############################################

    /**
     * Close the connection to the client
     */
    public void disconnectClient() {
        this.client.close();
    }

    /**
     * Try to connect the client to the cluster.
     */
    public void connectClient() {
        try {
            this.client = Ignition.start(this.clientConfig);
        } catch (IgniteException e) {
            System.out.println("Cause: " + e.getCause().getMessage());
            e.printStackTrace();
            System.out.println("Could not connect the client! Maybe it is already connected to the cluster.");
        }
    }

    /**
     * Randomly generates persons and puts data about them
     * @param p Maximum number of different persons that are generated (might be less persons than this value)
     * @param size Defines how many Ill tuples are generated for maximal p persons and stored
     */
    public void putRandomData(int p, int size) {

        // Get all terms
        ArrayList<String> terms = this.affinityFunction.getTerms();
        int numTerms = terms.size();

        // Get the info cache
        IgniteCache<InfoKey, Info> cacheInfo = this.client.getOrCreateCache(this.cacheConfigInfo);



        for (int i = 0; i < size; i++) {

            // Some random disease for a random personID (bound by argument p)
            int indexTerms = (new Random()).nextInt(numTerms);
            String disease = terms.get(indexTerms);
            int personID = (new Random()).nextInt(p);

            // Create IllKey- and Ill-Objects
            IllKey illKey = new IllKey(personID, disease);
            Ill ill = new Ill(illKey, "diseaseID123");      // TODO disease ID

            // put ill-tuple to cache ill_id according to clustering
            int fragID = this.affinityFunction.identifyCluster(disease);
            this.client.cache("ill_" + fragID).put(illKey, ill);

            // Add the info-object to the same partition (if not present yet with another disease of same cluster)
            InfoKey infoKey = new InfoKey(personID, fragID);
            SqlQuery sql = new SqlQuery(Info.class, "id = ?");
            List<Entry<InfoKey, Info>> list = cacheInfo.query(sql.setArgs(personID)).getAll();
            Info info;
            if (list.size() >= 1)       // The person's information already exist
                info = list.get(0).getValue();
            else                        // Generate a new person
                info = new Info(infoKey);
            cacheInfo.putIfAbsent(infoKey, info);
        }

    }


// ################################## MAIN ######################################################

    /**
     * Test unit
     * @param args Not used
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        // Affinity function, calculates clustering in constructor
        String separ = File.separator;
        String termsFile = "out" + separ + "csv" + separ + "terms30.txt";
        String simFile = "out" + separ + "csv" + separ + "result30.csv";
        MyAffinityFunction myAffinityFunction = new MyAffinityFunction(0.13, termsFile, simFile);

        // Setup (Configs, create Caches)
        IgniteSetupCaches setup = new IgniteSetupCaches(myAffinityFunction,
                Arrays.asList("192.168.1.1:47500..47509", "192.168.1.2:47500..47509"));


        // Put some data to test:
        Collection<IgniteCache> fragCaches = setup.client.getOrCreateCaches(Arrays.asList(setup.fragmentConfigs));
        for (IgniteCache c : fragCaches)
            c.clear();
        setup.client.getOrCreateCache(setup.cacheConfigInfo).clear();
        setup.putRandomData(1000, 4000);
        setup.disconnectClient();


        // Open the JDBC connection
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://192.168.1.1");
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery("SELECT p.ID, i.DISEASE FROM \"ill_0\".ILL i, \"info\".INFO p " +
                        "WHERE p.ID = i.PERSONID ORDER BY p.ID");

        System.out.println("All persons that have a disease similar to " + myAffinityFunction.getClusters().get(0).getHead());
        System.out.println("Adom="+myAffinityFunction.getClusters().get(0).getAdom());
        while (res.next()) {
            System.out.println(res.getString(1) + ", " + res.getString(2));
        }
        System.out.println("\n-------------------------------------------------------------------\n");


        res = stmt.executeQuery("SELECT p.ID, i.DISEASE FROM \"ill_1\".ILL i, \"info\".INFO p " +
                        "WHERE p.ID = i.PERSONID ORDER BY p.ID");

        System.out.println("All persons that have a disease similar to " + myAffinityFunction.getClusters().get(1).getHead());
        System.out.println("Adom="+myAffinityFunction.getClusters().get(1).getAdom());
        while (res.next()) {
            System.out.println(res.getString(1) + ", " + res.getString(2));
        }
        System.out.println("\n-------------------------------------------------------------------\n");

        // TODO test SQL (insert) queries maybe?

//        // Some test query (no rewriting yet) -> cache has to be identified for correct querying
//        String query = "Select * from \"ill0\".ill, \"info\".info where ill.personid=info.id ORDER BY ill.personid";
//        FieldsQueryCursor<List<?>> cursor = setup.client.cache().query(new SqlFieldsQuery(query));
//        System.out.println("Query-result:");
//        for (List<?> row : cursor) {
//            for (int i = 0; i < row.size(); i++) {
//                System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
//            }
//            System.out.println();
//        }
//
//        // query from right cache instance works without schema for ill fragment
//        query = "Select * from ill, \"info\".info where ill.personid=info.id ORDER BY ill.personid";
//        cursor = fragCaches.get(1).query(new SqlFieldsQuery(query));
//        System.out.println("Query-result:");
//        for (List<?> row : cursor) {
//            for (int i = 0; i < row.size(); i++) {
//                System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
//            }
//            System.out.println();
//        }
//
//
//        // Query across schemata i.e. join ill_0.ill and ill_1.ill --> needs distributed join (not collocated)
//        query = "Select Distinct i.personid from \"ill0\".ill i, \"ill1\".ill i2 WHERE i.personid=i2.personid ORDER BY i.personid";
//        SqlFieldsQuery qry = new SqlFieldsQuery(query);
//        qry.setDistributedJoins(true);      // enable distr join for this query
//        cursor = fragCaches.get(0).query(qry);
//        System.out.println("Query-result:");
//        for (List<?> row : cursor) {
//            for (int i = 0; i < row.size(); i++) {
//                System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
//            }
//            System.out.println();
//        }

            // Test collocate compute and data
//            IgniteCompute compute = ignite.compute();
//            compute.affinityCall("", 0, () -> {
//
//            });

        conn.close();
    }

}
