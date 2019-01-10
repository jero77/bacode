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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class IgniteSetupCachesTest {


    public static void main(String[] args) {


        // Affinity function
        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};      // active domain
        MyAffinityFunction<String> myAffinityFunction = new MyAffinityFunction<>(0.2, terms);

        // Cache configurations

        CacheConfiguration<IllKey, Ill> cacheConfigIll = new CacheConfiguration<>();
        cacheConfigIll.setName("ill")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(IllKey.class, Ill.class)
                .setAffinity(myAffinityFunction);

        CacheConfiguration<InfoKey, Info> cacheConfigInfo = new CacheConfiguration<>();
        cacheConfigInfo.setName("info")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(InfoKey.class, Info.class)
                .setAffinity(myAffinityFunction);


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

            IgniteCache<InfoKey, Info> cacheInfo = ignite.getOrCreateCache(cacheConfigInfo);
            System.out.format("Created/Got cache [%s]!\n", cacheConfigInfo.getName());

            // Clear caches & put some test data
            cacheIll.clear();
            cacheInfo.clear();
            for (int i = 0; i < 10; i++) {
                // Some random disease for a random personID
                String disease = terms[(new Random()).nextInt(terms.length)];
                int personID = (new Random()).nextInt(4);
                IllKey illKey = new IllKey(personID, disease);
                Ill ill = new Ill(illKey, "diseaseID123");
                cacheIll.put(illKey, ill);
                System.out.println("Added: " + ill);

                // Add the info-object to the same partition (if not present yet with another disease of same cluster)
                int p = myAffinityFunction.partition(illKey);
                InfoKey infoKey = new InfoKey(personID, p);
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


            // Some test query
            String query = "Select * from ill, \"info\".info where ill.personid=info.id";
            FieldsQueryCursor<List<?>> cursor = cacheIll.query(new SqlFieldsQuery(query));
            System.out.println("Query-result:");
            for (List<?> row : cursor) {
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(cursor.getFieldName(i) + ": " + row.get(i) + ", ");
                }
                System.out.println();
            }


        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

}
