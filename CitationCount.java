import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.lang.*;
import java.util.List;
import java.util.ArrayList;

public class CitationCount{
	
	public static class CitationMapper1 extends Mapper<LongWritable, Text, LongWritable, Text>{
		// private final static IntWritable one = new IntWritable(1);
		// private IntWritable citationCount = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] tokens = value.toString().split("\t");
			int fromNode = Integer.parseInt(tokens[0]);
			int toNode = Integer.parseInt(tokens[1]);
			context.write(new LongWritable(fromNode), new Text(String.valueOf(toNode * -1)));
			context.write(new LongWritable(toNode), new Text(String.valueOf(fromNode)));
		}
	}

	public static class CitationMapper2 extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] tokens = value.toString().trim().split(",");
			int keyNode;
			int count = 0;
			for(String token: tokens){
				count++;
				if(Integer.parseInt(token) < 0){
					keyNode = Integer.parseInt(token) * -1;
					List<String> list = new ArrayList<String>();
					for (int i=0; i < tokens.length; i++){
						list.add(tokens[i]);
					}
					list.remove(String.valueOf(keyNode * -1));
					list.add(String.valueOf(key));
					
					String str = new String();
					for(String v: list){
						str += " " + v.toString();
					}
					context.write(new LongWritable(keyNode), new Text(str));
				}
				else if (count == (tokens.length - 1)){
					context.write(key, value);
				}
			}
		}
	}

	public static class CitationReducer1 extends Reducer<LongWritable, Text, LongWritable, Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			String out = new String();
			for(Text val: values){
				out += "," +  val.toString();
			}
			context.write(key, new Text(out));
		}
	}
	
	public static class CitationReducer2 extends Reducer<LongWritable, Text, LongWritable, IntWritable>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			List<String> list = new ArrayList<String>();

			for (Text val: values){
			       	String[] tokens = val.toString().trim().split("\t");
				for (String token: tokens){
					if(!list.contains(token)){
						list.add(token);
					}
				}
			}

			context.write(key, new IntWritable(list.size()));
		}
	}

	public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "direct citation count");
		 job.setJarByClass(CitationCount.class);
		 job.setMapperClass(CitationMapper1.class);
		 job.setReducerClass(CitationReducer1.class);
		 job.setMapOutputKeyClass(LongWritable.class);
		 job.setMapOutputValueClass(Text.class);
		  
//		 // Job Control (1)
//		 ControlledJob ctrljob1 = new ControlledJob(conf);
//		 ctrljob1.setJob(job);
		  
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		 Job job2 = Job.getInstance(conf, "indirect citation count");
		 job2.setJarByClass(CitationCount.class);
		 job2.setMapperClass(CitationMapper2.class);
                 job2.setReducerClass(CitationReducer2.class);
		 job2.setMapOutputKeyClass(LongWritable.class);
		 job2.setMapOutputValueClass(Text.class);
		  
//		 // Job Control (2)
//		 ControlledJob ctrljob2=new ControlledJob(conf);
//		 ctrljob2.setJob(job2);
//		  
//		 // Job2 depends on the results from Job1
//		 ctrljob2.addDependingJob(ctrljob1);

		 // to ensure job2 mapper takes the input as key,value as produced by Reducer
		 // job2.setInputFormatClass(KeyValueTextInputFormat.class);
		 FileInputFormat.addInputPath(job2, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		 
//		 // Instantiation Job Control
//		 JobControl jobCtrl=new JobControl("myctrl");
		 
		 // Add all the jobs into job control
//		 jobCtrl.addJob(ctrljob1);
//		 jobCtrl.addJob(ctrljob2);
//		  
//		 Thread  t=new Thread(jobCtrl);
//		 t.start();
//		  
//		 while(true){   
//			 if(jobCtrl.allFinished()){
//				 jobCtrl.stop();   
//				 break;
//			 }  
//		  }
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
