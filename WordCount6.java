
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount6 {
	// nltk stopwords
	static String[] stop = new String[] { "ourselves", "hers", "between","yourself", "but", "again", "there", "about",
			"once", "during","out", "very", "having", "with", "they", "own", "an", "be", "some","for", "do", "its",
			"yours", "such", "into", "of", "most","itself", "other", "off", "is", "s", "am", "or", "who", "as","from",
			"him", "each", "the", "themselves", "until", "below","are", "we", "these", "your", "his", "through", "done",
			"nor","me", "were", "her", "more", "himself", "this", "down", "should","our", "their", "while", "above", 
			"both", "up", "to", "ours","had", "she", "all", "no", "when", "at", "any", "before", "them","same", "and",
			"been", "have", "in", "will", "on", "does","yourselves", "then", "that", "because", "what", "over", "why",
			"so", "can", "did", "not", "now", "under", "he", "you", "herself","has", "just", "where", "too", "only",
			"myself", "which", "those","i", "after", "few", "whom", "t", "being", "if", "theirs", "my","against", "a",
			"by", "doing", "it", "how", "further", "was","here", "than" };
	static ArrayList<String> stopwords = new ArrayList<String>(Arrays.asList(stop));

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				str = str.replaceAll("\\p{Punct}", ""); // first change removing punctuation
				str = str.replaceAll("[^A-Za-z0-9 ]", ""); // Second ChangeRemoving any other character except alpha numeric
				str = str.toLowerCase(); // Third Change changing the case of all the words
				if (str.length() > 0) { // fourth Change checking whether string is not a white space
					context.write(new Text(str), one);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		int uniqueCount = 0; // used for counting the number of unique words
		int totalCount = 0;// used for total number of words
		int totalCountAfterStop = 0; // used for count total number of words after stop words removal
		int uniqueCountAfterStop = 0; // used for count of unique words after stop word removal
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (!(stopwords.contains(key.toString()))) { // here removing the stop words.
				uniqueCountAfterStop++;
				totalCountAfterStop = totalCountAfterStop + sum;
			}
			result.set(sum);
			uniqueCount++;
			totalCount = totalCount + sum;
		}
		// method to write the result
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("Number of Unique Words in corpus before stop word removal :"),new IntWritable(uniqueCount));
			context.write(new Text("Number of Unique Words in corpus after stop word removal :"),new IntWritable(uniqueCountAfterStop));
			context.write(new Text("Number of Words before stop word removal in corpus are :"),new IntWritable(totalCount));
			context.write(new Text("Number of Words after stop word removal in corpus are :"),new IntWritable(totalCountAfterStop));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount6.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}