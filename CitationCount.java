import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable.Comparator;
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

public class CitationCount {

	public static class CitationMapper1 extends Mapper<Object, Text, LongWritable, Text> {
		// private final static IntWritable one = new IntWritable(1);
		// private IntWritable citationCount = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] nodes = value.toString().trim().split("\t");
			int fromNode = Integer.valueOf(nodes[0]);
			int toNode = Integer.valueOf(nodes[1]);
			context.write(new LongWritable(fromNode), new Text(String.valueOf(toNode * -1)));
			context.write(new LongWritable(toNode), new Text(String.valueOf(fromNode)));
		}
	}

	public static class CitationReducer1 extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String out = new String();
			for (Text val : values) {
				out += ("\t" + val.toString());
			}
			context.write(key, new Text(out));
		}
	}

	public static class CitationMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {

//		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			int keyNode1 = Integer.valueOf(tokens[0]);
			List<String> list = new ArrayList<String>();
			for (String t : tokens) {
				if (t.equals("") || t.equals(String.valueOf(keyNode1))) {
					continue;
				}
				if (Integer.parseInt(t) > 0) {
					list.add(t);
				}
			}
			String str = String.join("\t", list);
			context.write(new LongWritable(keyNode1), new Text(str));
			int keyNode2;
			int count = 0;
			for (String token : tokens) {
				count++;
				if (token.equals("")) {
					continue;
				}
				if (Integer.parseInt(token) < 0) {
					keyNode2 = Integer.parseInt(token) * -1;
					List<String> neList = list;

					if (!neList.contains(String.valueOf(keyNode1))) {
						neList.add(String.valueOf(keyNode1));
					}
					String neStr = String.join("\t", neList);
					context.write(new LongWritable(keyNode2), new Text(neStr));
				}
			}
		}
	}

	public static class CitationReducer2 extends Reducer<LongWritable, Text, LongWritable, IntWritable> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<String>();

			for (Text val : values) {
				String[] tokens = val.toString().trim().split("\t");
				for (String token : tokens) {
					if (token.equals("")) {
						continue;
					}
					list.add(token);
//					if (!list.contains(token)) {
//						list.add(token);
//					}
				}
			}

			context.write(key, new IntWritable(list.size()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "direct citation count");
		job1.setJarByClass(CitationCount.class);
		job1.setMapperClass(CitationMapper1.class);
		job1.setReducerClass(CitationReducer1.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);

		// Job Control (1)
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		Job job2 = Job.getInstance(conf, "indirect citation count");
		job2.setJarByClass(CitationCount.class);
		job2.setMapperClass(CitationMapper2.class);
		job2.setReducerClass(CitationReducer2.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(IntWritable.class);

		// Job Control (2)
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);

		// Job2 depends on the results from Job1
		ctrljob2.addDependingJob(ctrljob1);

		// to ensure job2 mapper takes the input as key,value as produced by Reducer
		// job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		// Instantiation Job Control
		JobControl jobCtrl = new JobControl("myctrl");

		// Add all the jobs into job control
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);

		Thread t = new Thread(jobCtrl);
		t.start();

		while (true) {
			if (jobCtrl.allFinished()) {
				jobCtrl.stop();
				break;
			}
		}

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
