package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Question2 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text word1 = new Text();
		private Text word2 = new Text();
		HashMap<String, String> posform = new HashMap<String, String>();
		HashMap<String, String> posmap = new HashMap<String, String>();

		@Override
		protected void setup(Context context) throws IOException {

			
			posform.put("C", "conjunction");
			posform.put("P", "preposition");
			posform.put("!", "interjection");
			posform.put("r", "pronoun");
			posform.put("D", "definite_article");
			posform.put("I", "indefinite_article");
			posform.put("o", "nominative");
			posform.put("N", "noun");
			posform.put("p", "plural");
			posform.put("h", "noun_phrase");
			posform.put("V", "verb(up)");
			posform.put("t", "verb(t)");
			posform.put("i", "verb(i)");
			posform.put("A", "adjective");
			posform.put("v", "adverb");

			String pos = context.getConfiguration().get("partsOfSpeech");

			Path p = new Path(pos);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			String z;
			String a[];
			while ((z = br.readLine()) != null) {
				a = z.split(((char) 65533 + ""));
				posmap.put(a[0], a[1]);

			}

		}

		private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			StringTokenizer itr = new StringTokenizer(cleanLine);

			int len = 0;
			while (itr.hasMoreTokens()) {
				String y = itr.nextToken();
				len = y.length();
				if (len >= 5) {
					if (posmap.containsKey(y)) {
						String s = posmap.get(y);
						if (s != "" && s != null && s.length() <= 2) {
							for (int i = 0; i < s.length(); i++) {
								Text ps = new Text(posform.get(s.charAt(i) + ""));
								word1.set(String.valueOf(len));
								word2.set(ps);
								context.write(word1, word2);

							}
						}

					}

				}
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
			Text res = new Text();
			int sum = 0;
			int noun = 0, plural = 0, noun_phrase = 0, verbup= 0, verbT = 0, verbI = 0, adjective = 0, adverb = 0,
					conjunction = 0, preposition = 0, interjection = 0, pronoun = 0, def_art = 0, ind_art = 0,
					nominative = 0;
			
			for (Text val : values) {
				sum = sum + 1;
				String value = val.toString();
				if (value.equals("noun")) {
					noun += 1;
				} else if (value.equals("plural")) {
					plural += 1;
				} else if (value.equals("noun_phrase")) {
					noun_phrase += 1;
				} else if (value.equals("verb(up)")) {
					verbup += 1;
				} else if (value.equals("verb(t)")) {
					verbT += 1;
				} else if (value.equals("verb(i)")) {
					verbI += 1;
				} else if (value.equals("adjective")) {
					adjective += 1;
				} else if (value.equals("adverb")) {
					adverb += 1;
				} else if (value.equals("conjunction")) {
					conjunction += 1;
				} else if (value.equals("preposition")) {
					preposition += 1;
				} else if (value.equals("interjection")) {
					interjection += 1;
				} else if (value.equals("pronoun")) {
					pronoun += 1;
				} else if (value.equals("definite_article")) {
					def_art += 1;
				} else if (value.equals("indefinite_article")) {
					ind_art += 1;
				} else if (value.equals("nominative")) {
					nominative += 1;
				}
				
			}
			res.set("Count of words for length : "+ key +" is "+ sum + "   Distribution: "
					+ "{" + " noun=" + noun + ", verb=" + verbup + ", adjective="
					+ adjective + ", adverb=" + adverb+ ", noun_phrase="+noun_phrase+", verbI="+verbI+ ", conjunction="+conjunction+""
							+ ", preposition="+preposition+", pronoun="+pronoun+
					", definitivearticle="+def_art+", indefinite_article="+ind_art+", nominative="+nominative +" }" );
			context.write(key, res);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] OtherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		/*
		 * conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		 * conf.set("yarn.resourcemanager.address",
		 * "cshadoop1.utdallas.edu:8032"); conf.set("mapreduce.framework.name",
		 * "yarn");
		 */

		conf.set("partsOfSpeech", OtherArgs[2].toString());

		Job job = Job.getInstance(conf, "POSWordCount");
		job.setJarByClass(Question2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		

		FileInputFormat.addInputPath(job, new Path(OtherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(OtherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
