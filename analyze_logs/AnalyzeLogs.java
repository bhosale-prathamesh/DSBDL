import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.util.*;

public class AnalyzeLogs{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

			String valueString = value.toString();
			String[] logLine = valueString.split("-");
			context.write(new Text(logLine[0]), one);
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text t_key, Iterator<IntWritable> values, Context context) throws IOException,InterruptedException {
			Text key = t_key;
			int frequencyOfUser = 0;
			while (values.hasNext()) {
				IntWritable value = (IntWritable) values.next();
				frequencyOfUser += value.get();
			}
			context.write(key, new IntWritable(frequencyOfUser));
		}
	}
    
    public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "analyzelogs");
		
		job.setJarByClass(AnalyzeLogs.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
	}
}
