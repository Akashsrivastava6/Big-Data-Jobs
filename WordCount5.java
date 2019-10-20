

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.*;

public class WordCount5 {

	
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	  
	  private String localname;
	  
    public void setup(Context context) throws InterruptedException, IOException {
        super.setup(context);
        localname = ((FileSplit)context.getInputSplit()).getPath().toString();// used for retrieving the file name
    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String str=itr.nextToken();
    	str=str.replaceAll("\\p{Punct}",""); // first change removing punctuation
    	str=str.replaceAll("[^A-Za-z0-9 ]", ""); //Second Change Removing any other character except alpha numeric
    	str=str.toLowerCase(); // Third Change changing the case of all the words
    	if (str.length()>0){   // fourth Change checking whether string is not a white space
    		context.write(new Text(str), new Text(localname));
      }
    }
    }
    }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {
    
    int uniqueCount=0; // variable to count number of unique terms
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
      ArrayList<String> list=new ArrayList<String>();
      for (Text val : values) {
        if(!list.contains(val.toString())){
        	list.add(val.toString());
        }
      }
      if(list.size()==1){ //if the length is one then those words appear only in one file
      uniqueCount++;
      }
          }
    // method to write final result
    public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text("Number of words that appear only in one file are:"), new IntWritable(uniqueCount));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount5.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}