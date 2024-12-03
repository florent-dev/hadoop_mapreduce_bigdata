
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question2_1 {
	public static final Integer DEFAULT_COUNT = 5;

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] flickr = value.toString().replaceAll("\\s*,\\s*$", "").trim().split("\\t");
			if (flickr[11].isEmpty() || flickr[10].isEmpty() || flickr[8].isEmpty()) {
				return;
			}
			Country country = Country.getCountryAt(Double.valueOf(flickr[11]), Double.valueOf(flickr[10]));
			
			if (country != null) {
				for (String userTag: java.net.URLDecoder.decode(flickr[8]).split(",")) {
					context.write(new Text(country.toString()), new Text(userTag));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private Integer maxCountryTagsSize;
		
		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
			maxCountryTagsSize = context.getConfiguration().getInt("maxCountryTagsSize", DEFAULT_COUNT);
	    }
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("reduce -> " + key);
			HashMap<String, Integer> tagsCount = new HashMap<String, Integer>();
			for (Text value: values) {
				if (tagsCount.containsKey(value.toString())) {
					tagsCount.put(value.toString(), (tagsCount.get(value.toString()) + 1));
				} else {
					tagsCount.put(value.toString(), 1);
				}
			}
			
	        MinMaxPriorityQueue<StringAndInt> queue = MinMaxPriorityQueue
	                .maximumSize(maxCountryTagsSize)
	                .create();

	        for (Map.Entry<String, Integer> entry : tagsCount.entrySet()) {
	            queue.add(new StringAndInt(entry.getKey().toString(), entry.getValue()));	            
	        }

	        queue.stream()
	                .sorted(Collections.reverseOrder()) // desc order
	                .forEach(tag -> {
						try {
							context.write(key, tag.getKey());
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}); // "country: tag" format
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		Integer maxCountryTagsSize = Integer.valueOf(otherArgs[2]);
		conf.setInt("maxCountryTagsSize", maxCountryTagsSize);
				
		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);
		
		// Mapper
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		// Combiner
		//job.setCombinerClass(MyCombiner.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}