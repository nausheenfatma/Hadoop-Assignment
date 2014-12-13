package pkg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

public class BusinessMapper extends Mapper<LongWritable,JSONObject,Text,IntWritable>{

	@Override
	protected void map(LongWritable inputKey,JSONObject inputValue,Context context){
		 Text city = new Text(inputValue.get("city").toString());
		 if(!(city.toString()).isEmpty()){
		 try {
			 context.write(city, new IntWritable(1));
		 	} catch (IOException e) {
		 		e.printStackTrace();
		 	} catch (InterruptedException e) {
		 		e.printStackTrace();
		 	}
		}
	}
	
}
