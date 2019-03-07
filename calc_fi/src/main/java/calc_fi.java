import java.io.*;

public class calc_fi {


    @SuppressWarnings("Duplicates")
    public static void true_test(String args,double arg2) throws IOException {


        InputStream instream = new FileInputStream(args);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(instream));
        String line = null;
        long truepositive = 0;
        long falsenegative = 0;

        while ((line = reader.readLine()) != null) {
            String[] splitline = line.split("\t");
            Double sim = Double.valueOf(splitline[2]);
            if (sim > arg2) {
                truepositive++;
            } else {
                falsenegative++;
            }
        }
        System.out.println("true posivtive = " + truepositive + " false negative = " + falsenegative);
    }


    @SuppressWarnings("Duplicates")
    private static void false_test(String args,double arg2) throws IOException {


        InputStream instream = new FileInputStream(args);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(instream));
        String line = null;
        long false_positive = 0;
        long true_negative = 0;

        while ((line = reader.readLine()) != null) {
            String[] splitline = line.split("\t");
            Double sim = Double.valueOf(splitline[2]);
            if (sim > arg2) {
                false_positive++;
            } else {
                true_negative++;
            }
        }
        System.out.println("true_negative = " + true_negative + " false_positive = " + false_positive);
    }

    public static void main(String[] args) throws IOException {
        true_test(args[0],Double.valueOf(args[2]));
        false_test(args[1],Double.valueOf(args[2]));
    }

}