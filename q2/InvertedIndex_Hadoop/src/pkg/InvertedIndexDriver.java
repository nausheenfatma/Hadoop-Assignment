package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndexDriver {
	
	private static Path inputPath=new Path("C:\\hadoopInput\\Input2.txt");
	private static Path outputPath=new Path("C:\\Output2");

	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "invertedindex");
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
	    
	   
	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
	        
	    job.waitForCompletion(true);
	 }
	  
}
