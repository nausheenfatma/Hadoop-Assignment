package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BusinessDriver {

	private static Path inputPath=new Path("C:\\hadoopInput\\Input3.txt");
	private static Path outputPath=new Path("C:\\Output3");

	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "businesscity");
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class); 
	    
	   
	    job.setMapperClass(BusinessMapper.class);
	    job.setReducerClass(BusinessReducer.class);
	        
	    job.setInputFormatClass(CustomTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
	        
	    job.waitForCompletion(true);
	 }
	  
}
