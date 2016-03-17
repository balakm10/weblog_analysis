package trg.hadoop.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WebLogWritable implements Writable {
	
	private String ip;
	private String date_time;
	private String request;
	private String response;
	private String bytes_sent;
	private String referer;
	private String browser;
	private String website;
	private String[] keywords;
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getDate_time() {
		return date_time;
	}

	public void setDate_time(String date_time) {
		this.date_time = date_time;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}

	public String getBytes_sent() {
		return bytes_sent;
	}

	public void setBytes_sent(String bytes_sent) {
		this.bytes_sent = bytes_sent;
	}

	public String getReferer() {
		return referer;
	}

	public void setReferer(String referer) {
		this.referer = referer;
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public String[] getKeywords() {
		return keywords;
	}

	public void setKeywords(String[] keywords) {
		this.keywords = keywords;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public WebLogWritable(String ip, String date_time, String request,
			String response, String bytes_sent, String referer, String browser) {
		super();
		this.ip = ip;
		this.date_time = date_time;
		this.request = request;
		this.response = response;
		this.bytes_sent = bytes_sent;
		this.referer = referer;
		this.browser = browser;
	}

	public WebLogWritable() {
		super();
		this.ip = new String();
		this.date_time = new String();
		this.request = new String();
		this.response = new String();
		this.bytes_sent = new String();
		this.referer = new String();
		this.browser = new String();
		//this.keywords = new String[1];
		this.website = new String();
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		ip = in.readUTF();
		date_time = in.readUTF();
		request = in.readUTF();
		response = in.readUTF();
		bytes_sent = in.readUTF();
		referer = in.readUTF();
		browser = in.readUTF();
		website = in.readUTF();
		int length = in.readInt();
		keywords = new String[length];
		for(int i=0; i<length; i++)
			keywords[i] = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(date_time);
		out.writeUTF(request);
		out.writeUTF(response);
		out.writeUTF(bytes_sent);
		out.writeUTF(referer);
		out.writeUTF(browser);
		out.writeUTF(website);
		int length = 0;
		if(keywords != null)
			length = keywords.length;
		out.writeInt(length);
		for(int i=0; i<length; i++)
			out.writeUTF(keywords[i]);
	}

	@Override
	public String toString() {
		return "WebLogWritable [ip=" + ip + ", date_time=" + date_time
				+ ", request=" + request + ", response=" + response
				+ ", bytes_sent=" + bytes_sent + ", referer=" + referer
				+ ", browser=" + browser + ", website=" + website
				+ ", keywords=" + Arrays.toString(keywords) + "]";
	}
	

}
