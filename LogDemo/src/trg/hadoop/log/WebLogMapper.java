package trg.hadoop.log;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WebLogMapper extends Mapper<Text, WebLogWritable, NullWritable, Text> {

	private WebLogParser webLogParser = new WebLogParser();
	private WebLogWritable webLogWritable;
	
	@Override
	protected void map(Text key, WebLogWritable value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String referer = value.getReferer();
		if(referer.length() > 1) {
			value.setKeywords(webLogParser.parseKeyword(referer));
			value.setWebsite(webLogParser.parseURI(referer));
		}
		else {
			String browser = value.getBrowser();
			value.setWebsite(webLogParser.parseURI(browser));
			value.setKeywords(new String[]{});
		}
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		context.write(NullWritable.get(), gson.toJson(value));
		
	}
}
