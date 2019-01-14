import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MeSHSimilarity {

    private static HashSet<String> terms;

    private final static int MAX_TERMS = 100;

    /**
     * Generates csv file with paris of terms for similarity calculation, uses out/csv/mesh.csv as input
     * @param args not used
     */
    public static void main(String[] args) {
        String separ = File.separator;
        String inputFile = "out" + separ + "csv" + separ + "mesh.csv";
        String line = "";

        // Save csv content to HashMap and
        HashMap<String, String> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            while ((line = reader.readLine()) != null) {
                String[] s = line.split("\\|");
                map.put(s[0], s[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int numTerms = map.size();
        String[] t = new String[numTerms];
        t = map.values().toArray(t);


        // Collect 300 randomly chosen terms from array t
        terms = new HashSet<String>(MAX_TERMS);
        Random random = new Random();
        int i;
        while (terms.size() < 100) {
            i = random.nextInt(numTerms);
            terms.add(t[i]);
        }


        // Save termpairs to csv for 10, 30 and 100 (=MAX_TERMS) terms
        int[] counts = {10, 30, 100};
        String outFilePrefix = "out" + separ + "csv" + separ + "termpairs";
        String outFileSuffix = ".csv";
        for (int c : counts)
            writeTerms(outFilePrefix, outFileSuffix, c);
        System.out.println("Done!");
    }


    /**
     * Write the first count terms of the static Set 'terms' to a csv named
     * '[outFilePrefix]count[outFileSuffix].csv]
     * @param outFilePrefix
     * @param outFileSuffix
     * @param count
     */
    private static void writeTerms(String outFilePrefix, String outFileSuffix, int count) {
        String[] t = terms.toArray(new String[MAX_TERMS]);
        try (FileOutputStream fileout = new FileOutputStream(outFilePrefix + count + outFileSuffix)) {
            for (int i = 0; i < count; i++) {
                for (int j = i + 1; j < count; j++)
                    fileout.write((t[i] + "<>" + t[j] + "\n").getBytes(StandardCharsets.UTF_8));
                fileout.flush();
            }
            fileout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
