package trg.hadoop.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
		
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(value.toString());
		if (!matcher.matches() || 
				 matcher.groupCount()!=9) {
			      System.err.println("Bad log entry (or problem with RE?):");
			      System.err.println(value.toString());
			      return;
			    }
		List<String> list = new  ArrayList<String>();
		list.add(matcher.group(4));
		list.add(matcher.group(5));
		list.add(matcher.group(6));
		list.add(matcher.group(7));
		list.add(matcher.group(8));
		list.add(matcher.group(9));
		context.write(new Text(matcher.group(1).toString()), new Text(list.toString()));
	}


}
