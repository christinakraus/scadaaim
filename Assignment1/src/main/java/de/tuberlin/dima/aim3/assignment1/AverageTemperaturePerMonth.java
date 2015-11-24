package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import de.tuberlin.dima.aim3.assignment1.FilteringWordCount.FilteringWordCountMapper;
import de.tuberlin.dima.aim3.assignment1.FilteringWordCount.WordCountReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class AverageTemperaturePerMonth extends HadoopJob {

	public static double minimumQuality;

	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> parsedArgs = parseArgs(args);

		Path inputPath = new Path(parsedArgs.get("--input"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));
		this.minimumQuality = minimumQuality;

		Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, AvgTemperatureMapper.class, Text.class,
				IntWritable.class, AvgTemperatureReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
		wordCount.waitForCompletion(true);

		return 0;
	}

	static class AvgTemperatureMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(line.toString());

			Text yearAndMonth = new Text();
			String keyString = itr.nextToken() + "\t" + itr.nextToken();
			int temperature = Integer.parseInt(itr.nextToken());
			boolean isOfBadQuality = false;
			double quality = Double.parseDouble(itr.nextToken());

			if (quality < minimumQuality) {
				isOfBadQuality = true;
			}
			if (!isOfBadQuality) {

				yearAndMonth.set(keyString);
				ctx.write(yearAndMonth, new IntWritable(temperature));

			}
			isOfBadQuality = false;
		}
	}

	static class AvgTemperatureReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
				throws IOException, InterruptedException {
			int sum = 0;
			int counter = 0;
			double resultCal;
			DoubleWritable result = new DoubleWritable();
			for (IntWritable val : values) {
				sum += val.get();
				counter++;
			}
			resultCal = sum*1.0/counter;
			result.set(resultCal);
			ctx.write(key, result);
		}
	}
}