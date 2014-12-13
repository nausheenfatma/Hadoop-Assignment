package pkg;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

public class BusinessReviewMapper extends Mapper<LongWritable,JSONObject,Text,IntWritable>{

	@Override
	protected void map(LongWritable inputKey,JSONObject inputValue,Context context){
		 Text reviewDate = new Text(inputValue.get("date").toString());;
		
		 if (!(reviewDate.toString()).isEmpty()) {
		 String tokens[]=(reviewDate.toString()).split("-");
		 String month = tokens[1];
		 writeToSeason(month, context);
		 writeToMonth(month, context);
		 }
		else{
			return;
		}
		 
	}
	
	
	
	public void writeToMonth(String month, Context context) {
		 
		String[] months = new String[] { "dummyMonth", "Jan", "Feb", "Mar", "Apr",
		 "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec" };
		 int monthNumber;
		 monthNumber = Integer.parseInt(month);
		 try {
		Text monthname=new Text();
		monthname.set(months[monthNumber]);
		 context.write(monthname, new IntWritable(1));
		 } catch (IOException e) {
		 e.printStackTrace();
		 } catch (InterruptedException e) {
		 e.printStackTrace();
		 }
		}
	
	public void writeToSeason(String month, Context context) {
		 
		 int monthNumber;
		 monthNumber = Integer.parseInt(month);
		 Text seasonName=new Text();
		 
		 if((monthNumber >=1 && monthNumber <3)||(monthNumber>=11)){
			 seasonName.set("Winter");;
		 }
		 else if(monthNumber >=3 && monthNumber <8)
		 {
			 seasonName.set("Spring");
		 }
		 else if(monthNumber >=8 && monthNumber <11){
			 seasonName.set("Monsoon");
		 }
		 try {

		 context.write(seasonName, new IntWritable(1));
		 } catch (IOException e) {
		 e.printStackTrace();
		 } catch (InterruptedException e) {
		 e.printStackTrace();
		 }
		}
	
}
