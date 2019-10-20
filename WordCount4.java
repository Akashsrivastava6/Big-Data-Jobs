

import java.io.IOException;
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
enum usercount{
	me_count,we_count;
};
public class WordCount4 {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String str=itr.nextToken();
    	str=str.replaceAll("\\p{Punct}",""); // first change removing punctuation
    	str=str.replaceAll("[^A-Za-z0-9 ]", ""); //Second Change Removing any other character except alpha numeric
    	str=str.toLowerCase(); // Third Change changing the case of all the words
    	if (str.length()>0){   // fourth Change checking whether string is not a white space
    		context.write(new Text(str), one);
      } } } }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
   
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      //checking the key and incrementing enumm value accordingly
      if(key.toString().equals("me")||key.toString().equals("mine")||key.toString().equals("my")||key.toString().equals("i")){
    	  context.getCounter(usercount.me_count).increment(sum);
      }
      else if(key.toString().equals("us")||key.toString().equals("our")||key.toString().equals("ours")||key.toString().equals("we")){
    	  context.getCounter(usercount.we_count).increment(sum);
      }
      context.write(key, result);

    }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount4.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}