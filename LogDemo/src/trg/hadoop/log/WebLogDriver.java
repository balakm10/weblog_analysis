package trg.hadoop.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebLogDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem hdfsfile = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "Web Log Anaysis");
		job.setJarByClass(getClass());
		
		try {
			DistributedCache.addFileToClassPath(new Path("/user/cloudera/exlib/gson-2.3.1.jar"), job.getConfiguration());
		} catch (Exception e) {
			System.err.println(e);
		}
		
		WebLogInputFormat.addInputPath(job, new Path(args[0]));
		hdfsfile.delete(new Path(args[1]), true);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(WebLogInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(WebLogMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new WebLogDriver(), args);
		System.exit(exitCode);
	}

}
