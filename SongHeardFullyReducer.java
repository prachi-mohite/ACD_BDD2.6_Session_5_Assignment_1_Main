import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongHeardFullyReducer extends Reducer<Text, IntWritable, Text, Text> {
	private int heardcount = 0; 
	private Text OutputofHeard ;
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
	{
		
		for (IntWritable value : values) {
			heardcount+=value.get();
		}
		OutputofHeard = new Text("fully Heard Songs Count " + Integer.toString(heardcount));
		context.write(key, OutputofHeard);
	}

}
