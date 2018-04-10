import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.varia.NullAppender;

public class UniqueListersMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Text lisnterID ;
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		lisnterID = new Text(parts[0]);	
		context.write(lisnterID, NullWritable.get());
	}

}
