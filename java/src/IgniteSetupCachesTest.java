import org.apache.commons.lang3.ArrayUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.Cache.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class IgniteSetupCachesTest {


    public static void main(String[] args) {


        // Affinity function, calculates clustering in constructor
        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};  // active domain
        MyAffinityFunction myAffinityFunction = new MyAffinityFunction(0.2);

        // Cache configurations for base tables
        CacheConfiguration<IllKey, Ill> cacheConfigIll = new CacheConfiguration<IllKey, Ill>();
        cacheConfigIll.setName("ill")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(IllKey.class, Ill.class)
                .setAffinity(myAffinityFunction);

        CacheConfiguration<InfoKey, Info> cacheConfigInfo = new CacheConfiguration<InfoKey, Info>();
        cacheConfigInfo.setName("info")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(InfoKey.class, Info.class)
                .setAffinity(myAffinityFunction);


        // Cache configurations for table fragments (cache ill[i] based on clustering)
        ArrayList<Cluster<String>> clustering = myAffinityFunction.getClusters();
        CacheConfiguration<IllKey, Ill>[] fragmentConfigs = new CacheConfiguration[clustering.size()];
        for (int id = 0; id < clustering.size(); id++) {
            Cluster cluster = clustering.get(id);
            CacheConfiguration<IllKey, Ill> cacheConfigIllFrag = new CacheConfiguration<IllKey, Ill>();
            cacheConfigIllFrag.setName("ill"+id)
                    .setBackups(0)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setIndexedTypes(IllKey.class, Ill.class)
                    .setAffinity(myAffinityFunction);
            fragmentConfigs[id] = cacheConfigIllFrag;
        }

        // Add base tables cache configurations and fragment configs to an array
        CacheConfiguration[]cacheConfigurations = ArrayUtils.addAll(fragmentConfigs, cacheConfigIll, cacheConfigInfo);

        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true)
            .setDiscoverySpi(spi)
            .setCacheConfiguration(cacheConfigurations)
            .setPeerClassLoadingEnabled(true);


        // Start client that connects to the cluster
        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Client started!");

            // create or get cache (key-value store)
//            IgniteCache<IllKey, Ill> cacheIll = ignite.getOrCreateCache(cacheConfigIll);
//            cacheIll.clear();
//            System.out.format("Created/Got cache [%s]!\n", cacheConfigIll.getName());

            IgniteCache<InfoKey, Info> cacheInfo = ignite.getOrCreateCache(cacheConfigInfo);
            cacheInfo.clear();
            System.out.format("Created/Got cache [%s]!\n", cacheConfigInfo.getName());

            // create or get all the caches for fragments
            ArrayList<IgniteCache<IllKey, Ill>> fragCaches = new ArrayList<IgniteCache<IllKey, Ill>>();
            for (int id = 0; id < clustering.size(); id++) {
                IgniteCache<IllKey, Ill> cacheFrag = ignite.getOrCreateCache(fragmentConfigs[id]);
                cacheFrag.clear();
                fragCaches.add(id, cacheFrag);
                System.out.printf("Created/Got cache [%s]!\n", cacheFrag.getName());
            }

            System.out.println("Cache Names:");         // DEBUG
            for (String s : ignite.cacheNames())
                System.out.println("\t" + s);


            // put test data
            for (int i = 0; i < 100; i++) {

                // Some random disease for a random personID
                String disease = terms[(new Random()).nextInt(terms.length)];
                int personID = (new Random()).nextInt(20);
                IllKey illKey = new IllKey(personID, disease);
                Ill ill = new Ill(illKey, "diseaseID123");

                // put ill-tuple to cache ill[id] according to clustering
                int fragID = myAffinityFunction.identifyCluster(disease);
                fragCaches.get(fragID).put(illKey, ill);
                System.out.println("Added: " + ill + " to the cache " +  fragCaches.get(fragID).getName());

                // Add the info-object to the same partition (if not present yet with another disease of same cluster)
                InfoKey infoKey = new InfoKey(personID, fragID);
                SqlQuery sql = new SqlQuery(Info.class, "id = ?");
                List<Entry<InfoKey, Info>> list = cacheInfo.query(sql.setArgs(personID)).getAll();
                Info info;
                if (list.size() >= 1)
                    info = list.get(0).getValue();
                else
                    info = new Info(infoKey);
                cacheInfo.putIfAbsent(infoKey, info);
                System.out.println("Added: " + info);

                System.out.println("-----------------------------------------------------------------------------");
            }


            // TODO test SQL commands maybe?



            // Some test query (no rewriting yet) -> cache has to be identified for correct querying
            String query = "Select * from \"ill0\".ill, \"info\".info where ill.personid=info.id ORDER BY ill.personid";
            FieldsQueryCursor<List<?>> cursor = fragCaches.get(0).query(new SqlFieldsQuery(query));
            System.out.println("Query-result:");
            for (List<?> row : cursor) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
                }
                System.out.println();
            }

            // query from right cache instance works without schema for ill fragment
            query = "Select * from ill, \"info\".info where ill.personid=info.id ORDER BY ill.personid";
            cursor = fragCaches.get(1).query(new SqlFieldsQuery(query));
            System.out.println("Query-result:");
            for (List<?> row : cursor) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
                }
                System.out.println();
            }


            // Query across schemata i.e. join ill_0.ill and ill_1.ill --> needs distributed join (not collocated)
            query = "Select Distinct i.personid from \"ill0\".ill i, \"ill1\".ill i2 WHERE i.personid=i2.personid ORDER BY i.personid";
            SqlFieldsQuery qry = new SqlFieldsQuery(query);
            qry.setDistributedJoins(true);      // enable distr join for this query
            cursor = fragCaches.get(0).query(qry);
            System.out.println("Query-result:");
            for (List<?> row : cursor) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
                }
                System.out.println();
            }

            // Test collocate compute and data
//            IgniteCompute compute = ignite.compute();
//            compute.affinityCall("", 0, () -> {
//
//            });


        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

}
