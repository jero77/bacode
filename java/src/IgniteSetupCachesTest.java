import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.*;

public class IgniteSetupCachesTest {


    public static void main(String[] args) {

        // Client configuration
        final String ADDRESSES = "127.0.0.1:47500..47509";
        ClientConfiguration cliConfig = new ClientConfiguration().setAddresses(ADDRESSES);

        // Cache configurations
        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};      // active domain
        CacheConfiguration cacheConfigIll = new CacheConfiguration();
        MyAffinityFunction<String> myAffinityFunction = new MyAffinityFunction<String>(2, 0.2, terms);
        cacheConfigIll.setName("ill")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(IllKey.class, Ill.class)
                .setAffinity(myAffinityFunction);

        CacheConfiguration cacheConfigInfo = new CacheConfiguration();
        cacheConfigInfo.setName("info")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Integer.class, Info.class);


        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true)
            .setDiscoverySpi(spi)
            .setCacheConfiguration(cacheConfigIll, cacheConfigInfo)
            .setPeerClassLoadingEnabled(true);


        // Start client that connects to the cluster
        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Client started!");

            // create or get cache (key-value store)
            IgniteCache<IllKey, Ill> cacheIll = ignite.getOrCreateCache(cacheConfigIll);
            System.out.format("Created/Got cache [%s]!\n", cacheConfigIll.getName());

            IgniteCache<Integer, Info> cacheInfo = ignite.getOrCreateCache(cacheConfigInfo);
            System.out.format("Created/Got cache [%s]!\n", cacheConfigInfo.getName());

            // TODO generate random data

            // put some test data
            cacheIll.clear();
            cacheInfo.clear();
            for (int i = 0; i < 5; i++) {
                // Some info
                Info info = new Info(i);
                cacheInfo.put(info.getId(), info);
                System.out.println("Added: " + info);

                // Some disease
                String disease = terms[(new Random()).nextInt(terms.length)];
                IllKey illKey = new IllKey(i, disease);
                Ill ill = new Ill(illKey, "diseaseID"+(112*i));
                cacheIll.put(illKey, ill);
                System.out.println("Added: " + ill + "\n");
            }


            // Some test query
            // TODO explain query failure --> maybe 2nd affinity function is needed for derived fragmentation?
            // TODO derived fragmentation
            // Failure: ignite maps info-objects randomly or maybe equally distributed but not collocated (yet)
            String query = "Select * from ill, \"info\".info where ill.personid=info.id";
            QueryCursor<List<?>> cursor = cacheIll.query(new SqlFieldsQuery(query));
            System.out.println("Query-result:");
            for (List<?> row : cursor) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(((FieldsQueryCursor<List<?>>) cursor).getFieldName(i) + ": " + row.get(i) + ", ");
                }
                System.out.println();
            }


        } catch (ClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
