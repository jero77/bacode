import java.io.*;

public class MeSHSimilarity {

    public static void main(String[] args) {
        String separ = File.pathSeparator;
        String inputFile = "out" + separ + "csv" + separ + " mesh.csv";
        String line = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {

            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
