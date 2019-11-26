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

public class EventPredictionPart3 {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
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
				if (!items.get(next).equals(items.get(current))) {
					MapWritable mapWritable = new MapWritable();

					while (next < items.size() && !items.get(next).equals(items.get(current))) {
						if (mapWritable.containsKey(new Text(items.get(next)))) {
							int newValue = ((IntWritable) mapWritable.get(new Text(items.get(next)))).get() + 1;
							mapWritable.put(new Text(items.get(next)), new IntWritable(newValue));
							//System.out.println("updating "+items.get(current)+" "+items.get(next)+ " "+mapWritable.get(new Text(items.get(next))));
						} else {
							mapWritable.put(new Text(items.get(next)), one);
							//System.out.println("inserting "+items.get(current)+" "+items.get(next)+ " "+mapWritable.get(new Text(items.get(next))));
						}
						next++;
					}
					context.write(new Text(items.get(current)), mapWritable);
					//System.out.println("emit MAPPER_KEY: ".concat(items.get(current)).concat(" MAPPER_VALUE: ".concat(mapWritable.toString())));
				}
			}
		}

	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			MapWritable combine = new MapWritable();
			for (MapWritable val : values) {
				for (Writable word : val.keySet()) {
					if (!combine.containsKey(word))
						combine.put(word, val.get(word));
					else {
						int newValue = ((IntWritable) combine.get(word)).get() + ((IntWritable) val.get(word)).get();
						combine.put(word, new IntWritable(newValue));
					}
				}
			}

			double total = 0;
			for (Writable word : combine.keySet())
				total += ((IntWritable) combine.get(word)).get();

			for (Writable word : combine.keySet()) {
				double newValue = ((IntWritable) combine.get(word)).get() / total;
				combine.put(word, new DoubleWritable(newValue));
			}

			context.write(key, combine);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "eventPrediction");
		job.setJarByClass(EventPredictionPart3.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

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