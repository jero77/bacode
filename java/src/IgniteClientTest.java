import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class IgniteClientTest {


    public static void main(String[] args) {

        // Client configuration
        final String ADDRESSES = "127.0.0.1:47500..47509";
        ClientConfiguration cliConfig = new ClientConfiguration().setAddresses(ADDRESSES);

        // Cache configuration
        final String CACHENAME = "default";
        CacheConfiguration cacheConfig = new CacheConfiguration();
        cacheConfig.setName(CACHENAME)
                .setBackups(0)
                .setTypes(Integer.TYPE, Integer.TYPE);

        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setDiscoverySpi(spi);

        // Start client that connects to the cluster
        //Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Client started!");

            // create or get cache (key-value store)
            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfig);
            System.out.format("Created/Got cache [%s]!\n", cacheConfig.getName());

            // put & get
//            for (int i = 0; i < 10; i++) {
//                cache.put(i, i * 120);
//            }
            HashSet<Integer> keys = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                keys.add(i);
            }

            Map<Integer, Integer> s = cache.getAll(keys);
            for (Integer val : s.values())
                System.out.println("val:" + val);


        } catch (ClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
