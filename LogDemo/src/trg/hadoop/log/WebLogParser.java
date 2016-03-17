package trg.hadoop.log;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class WebLogParser {
	
	private static String logPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (?:(\\d+)|\\S) \"([^\"]+)\" \"([^\"]+)\"";
	private static String refererPattern = "(.*?)q=(.*?)(?:&(.*?)|$)";
	private static String uriPattern = "([\\w:]+)//([\\w.]+)/";
	private static String datePattern = "^(.*?):(.*?)$";
	private Pattern pattern;
	private Matcher matcher;
	private WebLogWritable webLogWritable;
	private boolean badRecord;
	
	public void parseLog(String logEntry) throws ParseException {
		pattern = Pattern.compile(logPattern);
		matcher = pattern.matcher(logEntry);
		
		if(matcher.matches() && matcher.groupCount() == 9) {
			webLogWritable = new WebLogWritable(matcher.group(1), parseDate(matcher.group(4)), matcher.group(5), 
								matcher.group(6), matcher.group(7), matcher.group(8), matcher.group(9));
			badRecord = false;
		}
		
		else {
			      badRecord = true;
		}
	}
	
	public String parseDate(String date) throws ParseException {
		SimpleDateFormat sourceFormat = new SimpleDateFormat("dd/MMM/yyyy");
		Date mydate = sourceFormat.parse(date);
		return new SimpleDateFormat("yyyy-MM-dd").format(mydate).toString();
	}
	
	public String[] parseKeyword(String referer) throws UnsupportedEncodingException {
		pattern = Pattern.compile(refererPattern);
		matcher = pattern.matcher(referer);
		if(matcher.find()) {
			if(matcher.group(2)!=null){
				return java.net.URLDecoder.decode(matcher.group(2), "UTF-8").split(" ");
			}
		}
		return new String[]{};
	}
	
	public String parseURI(String uri) {
		pattern = Pattern.compile(uriPattern);
		matcher = pattern.matcher(uri);
		if(matcher.find()) {
			return matcher.group(2);
		}
		return "";
	}
	
	public WebLogWritable getWebLogWritable() {
		return webLogWritable;
	}

	public void setWebLogWritable(WebLogWritable webLogWritable) {
		this.webLogWritable = webLogWritable;
	}

	public boolean isBadRecord() {
		return badRecord;
	}

}