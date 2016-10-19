package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import map.FlowBean;

public class FlowSort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowSort.class);

		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("/Users/gengwuli/output"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/gengwuli/sortoutput"));

		boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}

class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	FlowBean k = new FlowBean();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\\\t");
		int len = split.length;
		k.set(Long.parseLong(split[len - 2]), Long.parseLong(split[len - 3]));
		context.write(k, value);
	}
}

class FlowSortReducer extends Reducer<FlowBean, Text, Text, NullWritable> {
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(values.iterator().next(), null);
	}
}
