package DA2SqlMapRed;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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


//id name profession experience
//mapper (map profession to details with experience >= 3)select,from ,where
//-> partitioner(split based on profession) group by
//-> reducer(limit to first 5 outputs , skip  first , also order by 
//ascending experience


public class SqlMapRed {
	// Map function
	  public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	    private Text word = new Text();
	    public void map(LongWritable key, Text value, Context context) 
	           throws IOException, InterruptedException{
	      String[] stringArr = value.toString().split("\\s+");
	      	if(Integer.parseInt(stringArr[3])>3) {
	      		word.set(stringArr[2]);
		        context.write(word, value);
	      	}          
	    }
	  }
	  //Partitioner
	  public static class MyPartitioner extends Partitioner<Text , Text>{		  
		  public int getPartition(Text key , Text value,int numReduceTasks) {
			  if(key.toString().equalsIgnoreCase("DataAnalyst"))return 0%numReduceTasks;		  
			  if(key.toString().equalsIgnoreCase("SolutionArchitect"))return 1%numReduceTasks;
			  return 2%numReduceTasks;			  
		  }
	  }
	  // Reducer function
		  public static class MyReducer extends Reducer<Text, Text, Text, Text>{  			  
			int count =0;  
		    private IntWritable result = new IntWritable();
		    public void reduce(Text key, Iterable<Text> values, Context context) 
		            throws IOException, InterruptedException {
		      
		      //SortedMap<Integer , String> m1 = new TreeMap<Integer,String>();
		      List<Integer> l1 = new ArrayList<Integer>();
		      List<String> l2 = new ArrayList<String>();
		      for (Text val : values) {
		    	  String[] stringArr = val.toString().split("\\s+");
		    	  l1.add(Integer.parseInt(stringArr[3]));
		    	  l2.add(val.toString());
		      }			      
		      for(int i=0;i<l1.size();i++) {
		    	  for(int j = 0 ; j<l1.size() -i -1;j++) {
		    		  if(l1.get(j)<l1.get(j+1)) {
		    			  int temp = l1.get(j);
		    			  l1.set(j,(l1.get(j+1)));
		    			  l1.set(j+1,temp);
		    			  String temp1 = l2.get(j);
		    			  l2.set(j,(l2.get(j+1)));
		    			  l2.set(j+1,temp1);
		    		  }
		    	  }
		      }
		      int count = 0;
		      for(String item : l2) {
		    	  context.write(new Text(""),new Text(item));
		    	  count++;
		    	  if(count ==5)break;
		      }
		      
		    }
		  }	    
	  public static void main(String[] args)  throws Exception{
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "WC");
		    job.setJarByClass(SqlMapRed.class);
		    job.setMapperClass(MyMapper.class);    
		    job.setReducerClass(MyReducer.class);
		    job.setPartitionerClass(MyPartitioner.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setNumReduceTasks(3);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
