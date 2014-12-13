package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{
		 @Override
		 public void reduce(Text inputKey, Iterable<Text> inputValues, Context context) throws IOException, InterruptedException {
		         StringBuffer sbf = new StringBuffer();
		         for (Object val : inputValues) {
		           sbf.append(val.toString());
		           sbf.append(",");
		           }
		         context.write(inputKey, new Text(sbf.toString()));
		    }		 
}
