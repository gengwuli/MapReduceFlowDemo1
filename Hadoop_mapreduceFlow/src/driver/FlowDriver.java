package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import junit.framework.Test;
import map.FlowBean;
import map.FlowMapper;
import reduce.FlowReducer;

public class FlowDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowDriver.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path("/Users/gengwuli/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/gengwuli/output"));

		boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
