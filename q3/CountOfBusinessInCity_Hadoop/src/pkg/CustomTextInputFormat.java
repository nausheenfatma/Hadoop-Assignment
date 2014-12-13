package pkg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.json.simple.JSONObject;


public class CustomTextInputFormat extends FileInputFormat<LongWritable, JSONObject>{
	 @Override
	    public RecordReader<LongWritable, JSONObject> createRecordReader(InputSplit split, TaskAttemptContext context) {
	        return new CustomRecordReader();
	    }

	
}
