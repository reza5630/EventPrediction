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

public class EventPredictionPart4 {

	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> items = new ArrayList<>();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens())
				items.add(tokenizer.nextToken());

			int next = 0;
			for (int current = 0; current < items.size() - 1; current++) {
				next = current + 1;
				while (next < items.size() && !items.get(next).equals(items.get(current))) {
					context.write(new Pair(new Text(items.get(current)), new Text(items.get(next))), one);
					next++;
				}
			}
		}
	}

	public static class Reduce extends Reducer<Pair, IntWritable, Text, MapWritable> {

		Text previousLeft;
		MapWritable combine;

		@Override
		protected void cleanup(Reducer<Pair, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			emit(context);
			super.cleanup(context);
		}

		@Override
		protected void setup(Reducer<Pair, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			previousLeft = new Text("");
			combine = new MapWritable();
		}

		public void reduce(Pair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			if (!previousLeft.equals(key.getLeft()) && !previousLeft.equals(new Text(""))) {
				emit(context);
			}
			previousLeft = new Text(key.getLeft());

			double sum = 0;
			for (IntWritable val : values) {
				sum++;
			}

			combine.put(new Text(key.getRight()), new DoubleWritable(sum));
		}

		private void emit(Context context) throws IOException, InterruptedException {
			double total = 0;
			double newValue = 0;
			for (Writable word : combine.keySet())
				total += ((DoubleWritable) combine.get(word)).get();

			for (Writable word : combine.keySet()) {
				newValue = ((DoubleWritable) combine.get(word)).get() / total;
				combine.put(word, new DoubleWritable(newValue));
			}

			context.write(previousLeft, combine);
			combine = new MapWritable();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "eventPrediction");
		job.setJarByClass(EventPredictionPart4.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}