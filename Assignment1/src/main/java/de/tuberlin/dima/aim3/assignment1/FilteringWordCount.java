package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class FilteringWordCount extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
        Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
     StringTokenizer itr = new StringTokenizer(line.toString().toLowerCase(), ";,. -");  
     Text word = new Text();
     String[] stopWords = {"to", "and", "in", "the"};
     IntWritable one = new IntWritable(1);
     boolean isStopWord=false;
     String wordString;
     while(itr.hasMoreTokens()){
    	 wordString = itr.nextToken().toLowerCase();
    	 for(int i = 0; i< stopWords.length;i++){
    		 String test = stopWords[i];
    		 if (wordString.equals(test)){
    			 isStopWord = true;
    			 break;
    		 }
    	 }
    	 
    	 if(!isStopWord){
    		 
    		 word.set(wordString);
    		 ctx.write(word, one);
    		 
    	 }
    	 isStopWord = false;
     }
    }
  }

  static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
        throws IOException, InterruptedException {
    	int sum = 0;
    	IntWritable result = new IntWritable();
    	for (IntWritable val:values){
    		sum += val.get();
    	}
    	result.set(sum);
    	ctx.write(key, result);
    }
  }

}