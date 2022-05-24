import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class weather{
	
	public static class MaxTempMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable arg0, Text Value, Context context)
		throws IOException, InterruptedException{
			String line = Value.toString();

			if (!(line.length() == 0)){
				String date = line.substring(6,14);
				
				float temp_Max = Float.parseFloat(line.substring(39, 45).trim()); 
				float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
				
				if (temp_Max > 35.0) {
				
					context.write(new Text("Hot Day " + date),new Text(String.valueOf(temp_Max))); 

            			}
 
				if (temp_Min < 10) {
				
					context.write(new Text("Cold Day " + date),new Text(String.valueOf(temp_Min)));   
			        }
			}
		}
 
	}
	
	
	public static class MaxTempReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text Key, Iterator<Text> Values, Context context)
		throws IOException, InterruptedException{
			String temperature = Values.next().toString();
			context.write(Key, new Text(temperature));
		}
	}
		
	
	public static void main(String[] args) throws Exception {
 
		Configuration conf = new Configuration();
				
		Job job = new Job(conf, "weather");
				                            
		job.setJarByClass(weather.class);  
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class); 
		job.setMapperClass(MaxTempMapper.class);  		
		job.setReducerClass(MaxTempReducer.class);  		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
 
		
		Path OutputPath = new Path(args[1]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		OutputPath.getFileSystem(conf).delete(OutputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
 
	}

}
