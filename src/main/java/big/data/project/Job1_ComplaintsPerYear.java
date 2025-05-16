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

public class Job1_ComplaintsPerYear {

    public static class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 9 && !parts[9].equals("COMPLAINT DATE")) {
                try {
                    String[] date = parts[9].split("/");
                    String year = date[2];
                    context.write(new Text(year), new IntWritable(1));
                } catch (Exception e) {}
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Complaints Per Year");
        job.setJarByClass(Job1_ComplaintsPerYear.class);
        job.setMapperClass(YearMapper.class);
        job.setCombinerClass(CommonReducer.SumReducer.class);
        job.setReducerClass(CommonReducer.SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
