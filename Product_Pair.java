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

public class Product_Pair {

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
			String totalKey;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			ArrayList<String> tokenlist = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				tokenlist.add(tokenizer.nextToken());
			}
			for (int i = 0; i < tokenlist.size(); i++) {
				String token1 = tokenlist.get(i);
				key=null;
				for (int j = i + 1; j < tokenlist.size()
						&& !tokenlist.get(j).equals(token1); j++) {
					String token2 = tokenlist.get(j);
					if (key == null) {
						key = token1 + ", %s";
					}
					totalKey = String.format(key, "*");
					if (assoc.containsKey(totalKey)) {
						assoc.put(totalKey, assoc.get(totalKey) + 1);
					} else {
						assoc.put(totalKey, 1);
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

		@Override
		public void reduce(Text key, Iterable<IntWritable> list, Context context)
				throws IOException, InterruptedException {
			//(key, *) must come first and save the total count
			int sum = 0;
			Iterator<IntWritable> values = list.iterator();
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (key.toString().indexOf('*') != -1) {
				total = sum;
			} else {
				context.write(key, new FloatWritable((float) sum / total));				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Product_Pair.class);
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
