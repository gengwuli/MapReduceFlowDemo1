package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
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
		
		//Can set partitioning rules so that keys can be separated into different reducers
		job.setPartitionerClass(new org.apache.hadoop.mapreduce.Partitioner<Text, FlowBean>() {
			//When return 1, it will be put to the 1st reducer
			//If return 2, it will be put to the 2nd reducer, etc
			@Override
			public int getPartition(Text key, FlowBean value, int numPartitions) {
				if (key.toString().equals("1")) {
					return 1;
				} else if (key.toString().equals("2")) {
					return 2;
				} else {
					return 0;
				}
			}

		}.getClass());
		//Can set number of reducers.
		job.setNumReduceTasks(6);
		
		//Delete output file if exists
		Path path = new Path("/path/you/want/to/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		//set input and output paths, the output path must not exists
		FileInputFormat.setInputPaths(job, new Path("/path/to/input"));
		FileOutputFormat.setOutputPath(job, new Path("/path/you/want/to/output"));

		//wait for complete
		boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
