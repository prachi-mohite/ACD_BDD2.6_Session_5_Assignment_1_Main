import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UniqueListers {
	public static void main(String[] args) throws Exception
	{
		//check if valid set of arguments is passed
				if(args.length < 2)
				{
					System.err.println("Unique Listners - Error  - Arguments are not properly passed.");
					System.exit(-1);
				}
				//get configuration details and create a job
				Configuration conf = new Configuration();
				Job job = new Job(conf);
				//set jar class
				job.setJarByClass(UniqueListers.class);
				//set input path and output path
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				Path outputPath = new Path(args[1]);
				FileOutputFormat.setOutputPath(job, outputPath);
				job.setNumReduceTasks(1);
				
				
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				
				   //execute the job
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
