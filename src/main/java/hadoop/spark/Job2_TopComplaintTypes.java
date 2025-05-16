package hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Job2_TopComplaintTypes {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top Complaint Types");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> data = lines.filter(line -> !line.startsWith("COMPLAINT ID"));

        JavaPairRDD<String, Integer> typeCounts = data.mapToPair(line -> {
            String[] parts = line.split(",", -1);
            if (parts.length <= 9 || parts[9].trim().isEmpty()) {
                return new Tuple2<>("Unknown", 1);
            }
            String type = parts[1];
            return new Tuple2<>(type, 1);
        }).reduceByKey(Integer::sum);

        typeCounts.saveAsTextFile(args[1]);
        sc.close();
    }
}
