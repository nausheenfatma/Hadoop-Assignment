package pkg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
	@Override
    protected void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : inputValues) {
            sum += val.get();
        }
        context.write(inputKey, new IntWritable(sum));
    }
}
