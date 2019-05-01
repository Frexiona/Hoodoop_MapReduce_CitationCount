import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.lang.*;

public class CitationCount{
	public static class CitationMapper1 extends Mapper<LongWritable, Text, LongWritable, Text>{
		// private final static IntWritable one = new IntWritable(1);
		// private IntWritable citationCount = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] tokens = value.toString().trim().split("\t");
			String fromNode = tokens[0];
			String toNode = tokens[1];
			context.write(new LongWritable(fromNode), new Text(String.valueOf(toNode * -1)));
		}
	}

	public static class CitationMapper2 extends Mapper<, LongWritable, LongWritable>

	public static class CitationReducer1 extends Reducer<LongWritable, Text, LongWritable, IntWritable>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			int count = 0;
			for(Text val : values){
				count++;
				//count += val.get();
			}

			context.write(key, new IntWritable(count));
		}
	}
	
	public static class CitationReducer2 extends Reducer<LongWritable, LongWritable, >

	  public static void main(String[] args) throws Exception{ 
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "direct citation count");
		  job.setJarByClass(CitationCount.class);
		  job.setMapperClass(CitationMapper1.class);
		  job.setReducerClass(CitationReducer1.class);
		  job.setMapOutputKeyClass(LongWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  FileInputFormat.addInputPath(job, new Path(arg0[0]));
		  FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		  Job job2 = Job.getInstance(conf, "indirect citation count");
		  job2.setJarByClass(CitationCount.class);
		  job.setMapperClass(CitationMapper2.class);
                  job.setReducerClass(CitationReducer2.class);
		  job2.setMapOutputKeyClass(LongWritable.class);
		  job2.setMapOutputValueClass(LongWritable.class);
		  // to ensure job2 mapper takes the input as key,value as produced by Reducer
		  job2.setInputFormatClass(KeyValueTextInputFormat.class);
		  FileInputFormat.addInputPath(job2, new Path(arg0[1]));
		  FileOutputFormat.setOutputPath(job2, new Path(arg0[2]));
  }
}
