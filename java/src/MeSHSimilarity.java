import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class MeSHSimilarity {

    public static void main(String[] args) {
        String separ = File.separator;
        String inputFile = "out" + separ + "csv" + separ + "mesh.csv";
        String line = "";

        HashMap<String, String> map = new HashMap<>();

        // Save csv content to HashMap
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


//        for (String key : map.keySet()) {
//            System.out.println(key + ": " + map.get(key));
//        }


        // Save termpairs to csv
        String[] terms = new String[1];
        terms = map.values().toArray(terms);
        String outFile = "out" + separ + "csv" + separ + "termpairs.csv";
        try (FileOutputStream fileout = new FileOutputStream(outFile)) {
            for (String t1 : terms) {
                for (String t2 : terms) {
                    fileout.write((t1 + "<>" + t2 + "\n").getBytes(StandardCharsets.UTF_8));
                }
                fileout.flush();
            }
            fileout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
