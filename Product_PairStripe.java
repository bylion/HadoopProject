import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Product_PairStripe {

	public static class Map
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

		private HashMap<String, Integer> assoc;

		public Map() {
			assoc = new HashMap<String, Integer>();
		}

		@Override
		public void map(LongWritable FID, Text value, Context output)
				throws IOException {
			String key = null;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			ArrayList<String> tokenlist = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				tokenlist.add(tokenizer.nextToken());
			}
			for (int i = 0; i < tokenlist.size(); i++) {
				String token1 = tokenlist.get(i);
				key = null;
				for (int j = i + 1; j < tokenlist.size()
						&& !tokenlist.get(j).equals(token1); j++) {
					String token2 = tokenlist.get(j);
					if (key == null) {
						key = token1 + ", %s";
					}
					if (!assoc.containsKey(String.format(key, token2))) {
						assoc.put(String.format(key, token2), 1);
					} else {
						assoc.put(String.format(key, token2),
								assoc.get(String.format(key, token2)) + 1);
					}

				}

			}
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws java.io.IOException, java.lang.InterruptedException {
			for (String key : assoc.keySet()) {
				context.write(new Text(key), new IntWritable(assoc.get(key)));
			}
		}
	}

	public static class CustomPartitioner<Text, IntWritable> extends
			org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			return (key.toString().split(",")[0]).hashCode() % numReduceTasks;
		}
	}

	public static class Reduce
			extends
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, FloatWritable> {
		int total;
		private HashMap<String, Integer> assoc;
		private String preKeyleft = "";

		public Reduce() {
			assoc = new HashMap<String, Integer>();
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> list, Context context)
				throws IOException, InterruptedException {
			// (key, *) must come first and save the total count
			String kString = key.toString().split(",")[0];
			if (!preKeyleft.equals(kString)) {
				if (!preKeyleft.equals("")) {
					Iterator<String> keyset = assoc.keySet().iterator();
					while (keyset.hasNext()) {
						String keyiterator = (String) keyset.next();
						int sum = assoc.get(keyiterator);
						context.write(new Text(preKeyleft + "," + keyiterator),
								new FloatWritable((float) sum / total));
					}
					total = 0;
					assoc = new HashMap<String, Integer>();
					preKeyleft = kString;
				} else {
					preKeyleft = kString;

				}

			}
			Iterator<IntWritable> values = list.iterator();
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			assoc.put(key.toString().split(",")[1], sum);
			total += sum;
		}

		
		
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, FloatWritable>.Context context)
				throws java.io.IOException, java.lang.InterruptedException {
			Iterator<String> keyset = assoc.keySet().iterator();
			while (keyset.hasNext()) {
				String keyiterator = (String) keyset.next();
				context.write(new Text(preKeyleft + "," + keyiterator),
						new FloatWritable((float) assoc.get(keyiterator)
								/ total));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Product_PairStripe.class);
		job.setJobName("Co existance");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
