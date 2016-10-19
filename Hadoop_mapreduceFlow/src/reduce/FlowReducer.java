package reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import map.FlowBean;

public class FlowReducer extends Reducer<Text, FlowBean, Text, NullWritable> {
	Text k = new Text();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		FlowBean flowBean = values.iterator().next();
		k.set(key.toString() + "\t"+flowBean.toString());
		context.write(k, null);
	}
}
