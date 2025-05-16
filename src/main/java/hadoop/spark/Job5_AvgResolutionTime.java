package hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Job5_AvgResolutionTime {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Average Resolution Time");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> data = lines.filter(line -> !line.startsWith("COMPLAINT ID"));

        JavaPairRDD<String, Tuple2<Integer, Integer>> durations = data.mapToPair(line -> {
            String[] parts = line.split(",", -1);
            if (parts.length <= 9 || parts[9].trim().isEmpty()) {
                return new Tuple2<>("Unknown", new Tuple2<>(1,1));
            }
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            try {
                Date start = sdf.parse(parts[9]);
                Date end = sdf.parse(parts[13]);
                long days = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24);
                return new Tuple2<>("avgDays", new Tuple2<>((int) days, 1));
            } catch (Exception e) {
                return new Tuple2<>("avgDays", new Tuple2<>(0, 0));
            }
        });

        Tuple2<String, Tuple2<Integer, Integer>> result = durations
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                .first();

        double average = result._2._2 == 0 ? 0 : (double) result._2._1 / result._2._2;
        System.out.println("Average Resolution Time (in days): " + average);

        sc.close();
    }
}
