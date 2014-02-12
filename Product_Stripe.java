import java.io.DataInput;
import java.io.DataOutput;
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

public class Product_Stripe {

	public static class Stripe extends MapWritable {
	    public void add(Stripe from) {
	        for (Writable fromKey : from.keySet()) {
	            if (containsKey(fromKey)) {
	                put(fromKey, new IntWritable(
	                		(
	                				(IntWritable) get(fromKey)
	                		).get() 
	                		+ 
	                		(
	                				(IntWritable) from.get(fromKey)).get()));
	            } else {
	                put(fromKey, from.get(fromKey));
	            }
	        }
	    }
	    @Override
	    public String toString() {
	        StringBuilder buffer = new StringBuilder("{");
	        for (Writable key : keySet()) {
	            buffer.append(key).append(":").append(get(key)).append(",");
	        }
	        return buffer.append("}").toString();
	    }
		public Writable put(String key, int value) {
			// TODO Auto-generated method stub
			return super.put(new Text(key), new IntWritable(value));
		}
		public Writable get(String key) {
			// TODO Auto-generated method stub
			return super.get(new Text(key));
		}
	}
	
	public static class Map
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Stripe> {
		private HashMap<String, Stripe> assoc;

		public Map() {
			assoc = new HashMap<String, Stripe>();
		}

		@Override
		public void map(LongWritable FID, Text value, Context output)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			ArrayList<String> tokenlist = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				tokenlist.add(tokenizer.nextToken());
			}
			for (int i = 0; i < tokenlist.size(); i++) {
				String token1 = tokenlist.get(i);
				Stripe  assoclocal;
				if(!assoc.containsKey(token1))
					assoclocal= new Stripe();
				else
					assoclocal=assoc.get(token1);
				
				for (int j = i + 1; j < tokenlist.size()
						&& !tokenlist.get(j).equals(token1); j++) {
					String token2 = tokenlist.get(j);
//					if (key == null) {
//						key = token1 + ", %s";
//					}
//					totalKey = String.format(key, "*");
					if (assoclocal.containsKey(new Text("*"))) {
						IntWritable t=(org.apache.hadoop.io.IntWritable) assoclocal.get("*");
						assoclocal.put("*", t.get()+1);
					} else {
						assoclocal.put("*", 1);
					}
					if (!assoclocal.containsKey(new Text(token2))) {
						assoclocal.put(token2, 1);
					} else {
						IntWritable t=(org.apache.hadoop.io.IntWritable) assoclocal.get(token2);
						assoclocal.put(token2,t.get()+1);
					}

				}
				assoc.put(token1, assoclocal);

			}
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Stripe>.Context context)
				throws java.io.IOException, java.lang.InterruptedException {
			for (String key : assoc.keySet()) {
				context.write(new Text(key), assoc.get(key));
			}
		}
	}

	public static class CustomPartitioner<Text, Stripe> extends
			org.apache.hadoop.mapreduce.Partitioner<Text, Stripe> {
		@Override
		public int getPartition(Text key, Stripe value, int numReduceTasks) {
			return (key.toString().split(",")[0]).hashCode() % numReduceTasks;
		}
	}

	public static class Reduce
			extends
			org.apache.hadoop.mapreduce.Reducer<Text, Stripe, Text, FloatWritable> {
		int total;

		@Override
		public void reduce(Text key, Iterable<Stripe> list, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<Stripe> values = list.iterator();
			Stripe sr=new Stripe();
			while (values.hasNext()) {
				sr.add(values.next());
			}
			if(sr.get(new Text("*"))!=null)
				total = ((IntWritable)sr.get(new Text("*"))).get();
			Iterator<Writable> keyset=sr.keySet().iterator();
			while (keyset.hasNext()) {
				Writable keyiterator=keyset.next();
				sum=((IntWritable)sr.get(keyiterator)).get();
				context.write(new Text(key.toString()+","+keyiterator), new FloatWritable((float) sum / total));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Product_Stripe.class);
		job.setJobName("Co existance");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Stripe.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	
}
