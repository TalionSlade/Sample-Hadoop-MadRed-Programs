package question2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class maxtrixsquare {
	// Map function
	static int  count = 0;
		  public static class MyMapper extends Mapper<LongWritable, Text,Text, Text>{
		    private Text word = new Text();
		    public void map(LongWritable key, Text value, Context context) 
		           throws IOException, InterruptedException {
		      String[] stringArr = value.toString().split("\\s+");
		      String newstr = "";
		      for(String s : stringArr) {
		    	  int n = Integer.parseInt(s);
		    	  int temp = n*n;
		    	  newstr += temp+" ";
		      }
		      word.set(newstr);
		      context.write(new Text(""+count),word);
		      count++;
		    }
		  }
		// Reduce function
		  public static class MyReducer extends Reducer<Text,Text,  Text, Text>{        
		    private IntWritable result = new IntWritable();
		    public void reduce(Text key, Iterable<Text > values, Context context) 
		            throws IOException, InterruptedException {
		      for (Text val : values) {
		        context.write(val , new Text(" "));
		      }    
		    }
		  }
		  //driver
		  public static void main(String[] args)  throws Exception{
			    Configuration conf = new Configuration();
			    Job job = Job.getInstance(conf, "WC");
			    job.setJarByClass(maxtrixsquare.class);
			    job.setMapperClass(MyMapper.class);
			    job.setReducerClass(MyReducer.class);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
		  
		  
}
