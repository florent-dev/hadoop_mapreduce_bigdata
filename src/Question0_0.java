
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

enum UserCounter{MISSING_LINES};

public class Question0_0 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		HashMap<String, Integer> inmemMap = null;
		Counter missingLinesCounter = null;
		
		@Override
		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			inmemMap  = new HashMap<String, Integer>();
			missingLinesCounter = (Counter) context.getCounter(UserCounter.MISSING_LINES);
		}
				
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().isEmpty()) {
				this.missingLinesCounter.increment(1);
			}
			
			for(String stringLoop : value.toString().split(" ")) {
				stringLoop = stringLoop.replaceAll("\\s*,\\s*$", "");
				stringLoop = stringLoop.trim();
				//context.write(new Text(stringLoop), new IntWritable(1));
				
				if (this.inmemMap.containsKey(stringLoop)) {
					this.inmemMap.put(stringLoop, (this.inmemMap.get(stringLoop) + 1));
				} else {
					this.inmemMap.put(stringLoop, 1);
				}
			}
		}
		
		@Override
		 public void cleanup(Context context) throws IOException, InterruptedException {
	        Iterator<Entry<String, Integer>> temp = this.inmemMap.entrySet().iterator();
	        while(temp.hasNext()) {
	            Entry<String, Integer> entry = temp.next();
	            String keyVal = entry.getKey() + "";
	            Integer countVal = entry.getValue();
	            context.write(new Text(keyVal), new IntWritable(countVal));
	        }
	    }

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question0_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//job.setCombinerClass(MyReducer.class);
		//job.setNumReduceTasks(3);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}