package pkg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
	
    private final int ZERO=0;
    
    @Override
	protected  void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
		final String[] records = StringUtils.split(inputValue.toString());  
		
		String retailer=records[ZERO];
		for(int i=1;i<records.length;i++){
			context.write(new Text(records[i]), new Text(retailer));
		}
	}

}
