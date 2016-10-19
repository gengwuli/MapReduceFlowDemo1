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
		//set default file system to be local 
		conf.set("fs.defaultFS", "file:///");
		//set mapreduce frame to be local instead of yarn
		conf.set("mapreduce.framework.name", "local");
		
		//get a new job instance, don't new Job();
		Job job = Job.getInstance(conf);

		//set jar then the framework can find
		job.setJarByClass(FlowDriver.class);
		
		//set mapper class
		job.setMapperClass(FlowMapper.class);
		
		//set reducer class
		job.setReducerClass(FlowReducer.class);
		
		//set input format class which is default to TextInputFormat
		//which tells the framework how to treat the input text
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//set map output class, if it's the same with reducer output
		//if it's the same with output class, omit it
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(FlowBean.class);

		//set output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		//set input and output paths, the output path must not exists
		FileInputFormat.setInputPaths(job, new Path("/path/to/input"));
		FileOutputFormat.setOutputPath(job, new Path("/path/you/want/to/output"));

		//wait for complete
		boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
