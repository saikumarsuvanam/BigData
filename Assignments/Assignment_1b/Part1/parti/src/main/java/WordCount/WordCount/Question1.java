package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		HashSet<String> positiveWords = new HashSet<String>();
		HashSet<String> negativeWords = new HashSet<String>();

		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

		@Override
		protected void setup(Context context) {

			String positive = context.getConfiguration().get("positive");
			String negative = context.getConfiguration().get("negative");
			// Configuration conf = context.getConfiguration();
			try {
				get(positiveWords, positive);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				get(negativeWords, negative);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer itr = new StringTokenizer(cleanLine);

			String val = null;
			while (itr.hasMoreTokens()) {
				val = itr.nextToken();
				if (positiveWords.contains(val)) {
					word.set("positive");
					context.write(word, one);
				}
				if (negativeWords.contains(val)) {
					word.set("negative");
					context.write(word, one);
				}
			}
		}

		public static void get(HashSet<String> classify, String path) throws IOException {
			Path dir = new Path(path);
			FileSystem filesystem = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(filesystem.open(dir)));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (!line.contains(";") || !line.isEmpty()) {
					classify.add(line);
				}
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		/*
		 * conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		 * conf.set("yarn.resourcemanager.address",
		 * "cshadoop1.utdallas.edu:8032"); conf.set("mapreduce.framework.name",
		 * "yarn");
		 */

		config.set("positive", otherArgs[2].toString());
		config.set("negative", otherArgs[3].toString());

		Job job = Job.getInstance(config, "WordCount");
		job.setJarByClass(Question1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputDirRecursive(job, true); 
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));


		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
