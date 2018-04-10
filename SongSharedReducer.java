import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SongSharedReducer extends Reducer<NullWritable, IntWritable, NullWritable, Text> {
	private int heardcount = 0; 
	private Text OutputofHeard ;
	@Override
	public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException , InterruptedException
	{
		
		for (IntWritable value : values) {
			heardcount+=value.get();
		}
		OutputofHeard = new Text("Song shared for " + Integer.toString(heardcount));
		context.write(NullWritable.get(), OutputofHeard);
	}

}
