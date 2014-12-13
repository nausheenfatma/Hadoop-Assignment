package pkg;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRecordReader extends RecordReader<LongWritable, JSONObject> {
	private static final Logger LOG = LoggerFactory.getLogger(CustomRecordReader.class);
	private LineRecordReader reader = new LineRecordReader();
	private static JSONObject value = new JSONObject();
	private final JSONParser jsonParser = new JSONParser();
	 
	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
        }
	}	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {	
		while (reader.nextKeyValue()) {
			 value.clear();
			
			 try {
			 if (parseLineToJSON(jsonParser, reader.getCurrentValue())) {
			 return true;
			 }
			 } catch (ParseException e) {
			 e.printStackTrace();
			 LOG.info("Parse Erorr", e.toString());
			 }
			 }
			 return false;}
	
	public static boolean parseLineToJSON(JSONParser parser, Text line) throws org.json.simple.parser.ParseException, ParseException {
			 try {
			 value = (JSONObject) parser.parse(line.toString());
			return true;
			 } catch (NumberFormatException e) {
			 LOG.warn("Parsing Error in Number Field" + line, e);
			 return false;
			 }
			 }

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return null;
	}

	@Override
	public JSONObject getCurrentValue() throws IOException, InterruptedException {
      return value;
}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

}
