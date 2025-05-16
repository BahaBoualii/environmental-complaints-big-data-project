package hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Job1_ComplaintsPerYear {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Complaints Per Year");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> data = lines.filter(line -> !line.startsWith("COMPLAINT ID"));

        JavaPairRDD<String, Integer> yearCounts = data.mapToPair(line -> {
            String[] parts = line.split(",", -1);
            if (parts.length <= 9 || parts[9].trim().isEmpty()) {
                return new Tuple2<>("Unknown", 1);
            }
            String[] dateParts = parts[9].split("/");
            String year = dateParts.length == 3 ? dateParts[2] : "Unknown";
            return new Tuple2<>(year, 1);
        }).reduceByKey(Integer::sum);

        yearCounts.saveAsTextFile(args[1]);
        sc.close();
    }
}
