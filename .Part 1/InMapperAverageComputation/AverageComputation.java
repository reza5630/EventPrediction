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

	public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
		HashMap<Text, Pair> hash;

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
				if (hash.containsKey(ip)) {
					hash.put(ip,
							new Pair(hash.get(ip).getValue() + Integer.parseInt(weight), hash.get(ip).getCount() + 1));
					System.out.println("put " + ip + " " + hash.get(ip));
				} else {
					hash.put(ip, new Pair(Integer.parseInt(weight), 1));
					System.out.println("put " + ip + " " + hash.get(ip));
				}
//				context.write(ip, new IntWritable(Integer.parseInt(weight)));
			}
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			hash = new HashMap<Text, Pair>();
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for (Text word : hash.keySet()) {
				context.write(word, hash.get(word));
				System.out.println("emit " + word + " " + hash.get(word));
			}
		}

	}

	public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (Pair val : values) {
				sum += val.getValue();
				count += val.getCount();
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(AverageComputation.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}