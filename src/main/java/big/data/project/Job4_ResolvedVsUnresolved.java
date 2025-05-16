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

public class Job4_ResolvedVsUnresolved {
    public static class ResolutionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 12 && !parts[12].equals("RESOLUTION")) {
                if (parts[12].trim().isEmpty()) {
                    context.write(new Text("Unresolved"), new IntWritable(1));
                } else {
                    context.write(new Text("Resolved"), new IntWritable(1));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Resolved vs Unresolved");
        job.setJarByClass(Job4_ResolvedVsUnresolved.class);
        job.setMapperClass(ResolutionMapper.class);
        job.setCombinerClass(CommonReducer.SumReducer.class);
        job.setReducerClass(CommonReducer.SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
