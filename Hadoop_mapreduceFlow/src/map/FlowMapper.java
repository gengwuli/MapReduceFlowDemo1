package map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	Text k = new Text();
	FlowBean v = new FlowBean();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\\t");
		int len = fields.length;
		String phoneNumber = fields[1];
		String upFlow = fields[len - 3];
		String downFlow = fields[len - 2];
		k.set(phoneNumber);
		v.set(Long.parseLong(upFlow), Long.parseLong(downFlow));
		context.write(k, v);
	}
}
