package big.data.project;

import big.data.project.utils.CommonReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Job5_AvgResolutionTime {
    public static class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            try {
                if (!parts[9].equals("COMPLAINT DATE") && !parts[13].trim().isEmpty()) {
                    Date start = sdf.parse(parts[9]);
                    Date end = sdf.parse(parts[13]);
                    long diff = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24); // days
                    context.write(new Text("AverageDays"), new IntWritable((int) diff));
                }
            } catch (Exception e) {}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Resolution Time");
        job.setJarByClass(Job5_AvgResolutionTime.class);
        job.setMapperClass(TimeMapper.class);
        job.setReducerClass(CommonReducer.AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
