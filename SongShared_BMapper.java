import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SongShared_BMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
	private IntWritable HeardFully = new IntWritable();
	int heardCount = 0;
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		if (parts[3].equalsIgnoreCase("1"))
		{
			heardCount++;
		}		
	
	}
	
	@Override
	public void cleanup(Context context) throws IOException , InterruptedException
	{
		HeardFully.set(heardCount);
		context.write(NullWritable.get(), HeardFully);
	}

}
