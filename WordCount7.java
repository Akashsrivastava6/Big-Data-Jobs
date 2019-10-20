
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class WordCount7 {
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			str = str.replaceAll("\\p{Punct}", ""); // first change removing punctuation
			str = str.replaceAll("[^A-Za-z0-9 ]", ""); // Second ChangeRemoving any other character except alpha numeric
			str = str.toLowerCase(); // Third Change changing the case of all the words
			StringTokenizer itr = new StringTokenizer(str);
			while (itr.hasMoreTokens()) {
				String token=itr.nextToken();
				if(token.equals("war")){
				for(int i=0;i<5;i++){
					if(itr.hasMoreTokens()){
					context.write(new Text(itr.nextToken()), one); // Writing the next five words to intermediate output 
				}}
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		HashMap<String, Integer> mapCount = new HashMap<String,Integer>();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();	}
			mapCount.put(key.toString(), sum);}
		// method to write the result
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			//making list of map elements
			List<Map.Entry<String,Integer>> sorted=new LinkedList<Map.Entry<String,Integer>>(mapCount.entrySet());
			//sorting the list
			Collections.sort(sorted,new Comparator<Map.Entry<String,Integer>>(){
				public int compare(Map.Entry<String,Integer> c1,Map.Entry<String,Integer> c2)
				{	return(c2.getValue()).compareTo(c1.getValue());	}});
			//making map from list
			int ck=0;
			for(Map.Entry<String, Integer> mp :sorted){
				String kii=mp.getKey();
				int val=mp.getValue();
				if(ck<5){
					context.write(new Text(kii), new IntWritable(val));
					ck++;
				}
				else{
					break;
				}}}}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount7.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}