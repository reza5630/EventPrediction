import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputation {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
//		HashMap<Text, IntWritable> hash;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text ip = new Text();
			String weight = "";
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			ip.set(tokenizer.nextToken());
			if (!(line.charAt(line.length() - 1) == '-')) {
				while (tokenizer.hasMoreTokens()) {
					weight = tokenizer.nextToken();
				}
				context.write(ip, new IntWritable(Integer.parseInt(weight)));
			}
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(AverageComputation.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}