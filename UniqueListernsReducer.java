import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UniqueListernsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	public void reduce(Text key,Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
	{
		  context.write(key, NullWritable.get());
	}

}
