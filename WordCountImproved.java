import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WordCountImproved extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_SEP = Pattern.compile("[\\p{Punct}\\s]+");

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] tokens = WORD_SEP.split(value.toString());
    			for (String t : tokens) {
        			if (t.length() > 0) {
            				word.set(t.toLowerCase());
            				context.write(word, one);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
