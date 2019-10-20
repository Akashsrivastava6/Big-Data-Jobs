

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

public class WordCount3 {

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
      }
    }
    }
    }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
    int uniqueCount=0; //variable to count the number of unique words
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      // if condition to check if the number of occurence of a word is less than 4 or not
      if(sum<4){
    	  uniqueCount++; // incrementing the count if number of occurence is less than 4
    	 // context.write(key, result);

      }
    }
    // filnally writting the count of words that have occurence less than 4
    public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text("Number of Unique Words with occurence less tahn 4 in corpus are :"), new IntWritable(uniqueCount));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);     ye bhi hai
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}