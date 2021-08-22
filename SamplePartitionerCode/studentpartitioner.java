package question1;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//partitions
//less 15 , 15-17,17-20, 20-23 , 23-25,25-27,27-30,30-33,33-greater
public class studentpartitioner {
	// Map function
	  public static class MyMapper extends Mapper<LongWritable, Text,IntWritable, Text>{
	    private Text word = new Text();
	    public void map(LongWritable key, Text value, Context context) 
	           throws IOException, InterruptedException {
	      String[] stringArr = value.toString().split("\\s+");
//	      word.set(stringArr[3]);
	      context.write(new IntWritable(Integer.parseInt(stringArr[2])),value);
	    }
	  }
	  //partitioner
	  public static class MyPartitioner extends Partitioner<IntWritable , Text>{
		  public int getPartition(IntWritable key , Text value,int numReduceTasks) {
			  if(key.get() <15)return 0%numReduceTasks;		  
			  else if(key.get() <17)return 1%numReduceTasks;
			  else if(key.get() <20)return 2%numReduceTasks;
			  else if(key.get() <23)return 3%numReduceTasks;
			  else if(key.get() <25)return 4%numReduceTasks;
			  else if(key.get() <27)return 5%numReduceTasks;
			  else if(key.get() <30)return 6%numReduceTasks;
			  else if(key.get() <33)return 7%numReduceTasks;
			  else return 8%numReduceTasks;
		  }
	  }
	  // Reduce function
	  public static class MyReducer extends Reducer<IntWritable,Text,  Text, IntWritable>{        
	    private IntWritable result = new IntWritable();
	    public void reduce(IntWritable  key, Iterable<Text > values, Context context) 
	            throws IOException, InterruptedException {
	      for (Text val : values) {
	        context.write(val , key);
	      }    
	    }
	  }
	  //driver
	  public static void main(String[] args)  throws Exception{
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "WC");
		    job.setJarByClass(studentpartitioner.class);
		    job.setMapperClass(MyMapper.class); 
		    job.setPartitionerClass(MyPartitioner.class);
		    job.setNumReduceTasks(9);
		    job.setReducerClass(MyReducer.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	  

}
