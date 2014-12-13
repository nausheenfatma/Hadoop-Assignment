package pkg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();
    
    @Override  
   protected void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
	   String line = inputValue.toString();
       StringTokenizer tokenizer = new StringTokenizer(line);
       while (tokenizer.hasMoreTokens()) {
    	   outputKey.set(tokenizer.nextToken());
    	   IntWritable outputValue=one;
           context.write(outputKey, outputValue);
       }
   }

}
