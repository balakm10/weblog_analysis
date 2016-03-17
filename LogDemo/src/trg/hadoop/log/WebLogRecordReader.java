package trg.hadoop.log;

import java.io.IOException;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class WebLogRecordReader extends RecordReader<Text, WebLogWritable> {

	private LineRecordReader lineReader;
	private Text key;
	private WebLogWritable value;
	
	@Override
	public void close() throws IOException {
		if (lineReader != null) {
            lineReader.close();
        }
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public WebLogWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineReader = new LineRecordReader();
		value = new WebLogWritable();
		key = new Text();
		lineReader.initialize(input, context);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!lineReader.nextKeyValue()) {
			return false;
		}
		
		String logEntry = lineReader.getCurrentValue().toString();
		
		WebLogParser webLogParser = new WebLogParser();
		try {
			webLogParser.parseLog(logEntry);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		if(webLogParser.isBadRecord()) {
		      System.err.println("Bad log entry (or problem with RE?):");
		      System.err.println(logEntry);
		}
		
		else {
			value = webLogParser.getWebLogWritable();
			key.set(value.getIp());
			System.out.println(value);
		}
		
		return true;
	}

}
