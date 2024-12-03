
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question3_0 {
	public static final Integer DEFAULT_COUNT = 5;

	public static class MyFirstMapper extends Mapper<LongWritable, Text, StringAndString, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] flickr = value.toString().replaceAll("\\s*,\\s*$", "").trim().split("\\t");
			if (flickr[11].isEmpty() || flickr[10].isEmpty() || flickr[8].isEmpty()) {
				return;
			}
			Country country = Country.getCountryAt(Double.valueOf(flickr[11]), Double.valueOf(flickr[10]));
			
			if (country != null) {
				for (String userTag: java.net.URLDecoder.decode(flickr[8]).split(",")) {
					context.write(new StringAndString(new Text(country.toString()), new Text(userTag)), new IntWritable(1));
				}
			}
		}
	}

	public static class MyFirstCombiner extends Reducer<StringAndString, IntWritable, StringAndString, IntWritable> {
		@Override
		protected void reduce(StringAndString key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("reduce -> " + key);
			int counter = 0;
			for (IntWritable nb: values) {
				counter += nb.get();
			}
	        context.write(key, new IntWritable(counter));
		}
	}

	public static class MyFirstReducer extends Reducer<StringAndString, IntWritable, StringAndString, IntWritable> {	
		@Override
		protected void reduce(StringAndString key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("reduce -> " + key);
			int counter = 0;
			for (IntWritable nb: values) {
				counter += nb.get();
			}
	        context.write(key, new IntWritable(counter));
		}
	}
	
	public static class MySecondMapper extends Mapper<StringAndString, IntWritable, StringAndIntWritableComparable, Text> {
		@Override
		protected void map(StringAndString key, IntWritable value, Context context) throws IOException, InterruptedException {			
			context.write(new StringAndIntWritableComparable(key.getCountry().toString(), value.get()), key.getTag());						
		}
	}
	
	public static class MySecondReducer extends Reducer<StringAndIntWritableComparable, Text, Text, StringAndIntWritableComparable> {
		private Integer maxCountryTagsSize;
		
		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
			maxCountryTagsSize = context.getConfiguration().getInt("maxCountryTagsSize", DEFAULT_COUNT);
	    }
		
		@Override
		protected void reduce(StringAndIntWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int tagCounter = 0;
		    System.out.println("reduce: " + key.toString());
		    for (Text value: values) {
		        // Write the country key and a new StringAndInt with the tag and its count
		        context.write(new Text(key.getStr()), new StringAndIntWritableComparable(value.toString(), key.getCount()));
		        tagCounter++;
		        
		        // Do not go further than the asked max country tags size
		        if (tagCounter >= maxCountryTagsSize) {
		            break;
		        }
		    }
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		Integer maxCountryTagsSize = Integer.valueOf(otherArgs[2]);
		conf.setInt("maxCountryTagsSize", maxCountryTagsSize);
				
		Job job = Job.getInstance(conf, "Question3_0");
		job.setJarByClass(Question3_0.class);
		
		job.setMapperClass(MyFirstMapper.class);
		job.setMapOutputKeyClass(StringAndString.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyFirstCombiner.class);
		job.setReducerClass(MyFirstReducer.class);
		job.setOutputKeyClass(StringAndString.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // question 21, Hadoop binary format
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path("q3_0_intermediate"));
		job.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf, "Question3_0");
		job2.setJarByClass(Question3_0.class);
		
		job2.setMapperClass(MySecondMapper.class);
		job2.setMapOutputKeyClass(StringAndIntWritableComparable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(MySecondReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(StringAndIntWritableComparable.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class); // question 23
		job2.setOutputFormatClass(TextOutputFormat.class); // question 23

		job2.setGroupingComparatorClass(GroupWritableComparator.class);
		job2.setSortComparatorClass(SortWritableComparator.class);
		
		FileInputFormat.addInputPath(job2, new Path("q3_0_intermediate"));
		FileOutputFormat.setOutputPath(job2, new Path(output));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}