import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

    /** Mapper: JSON 한 줄에서 subreddit, score 추출 -> (subreddit, (1, score)) */
    public static class ExtractMapper
            extends Mapper<Object, Text, Text, LongPairWritable> {

        private final Text outKey = new Text();
        private final LongPairWritable outVal = new LongPairWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                JSONObject obj = new JSONObject(value.toString());

                if (obj.has("subreddit") && obj.has("score")) {
                    String subreddit = obj.getString("subreddit").toLowerCase();
                    long score = ((Number) obj.get("score")).longValue();

                    outKey.set(subreddit);
                    outVal.set(1L, score); // count=1, sum=score
                    context.write(outKey, outVal);
                }
            } catch (Exception e) {
                // JSON 파싱 실패 → 스킵
            }
        }
    }

    /** Combiner: 같은 키의 (count,sum) 쌍을 부분 합산해서 셔플 트래픽 감소 */
    public static class SumPairsCombiner
            extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        private final LongPairWritable out = new LongPairWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            long sum   = 0;
            for (LongPairWritable p : values) {
                count += p.get_0();
                sum   += p.get_1();
            }
            out.set(count, sum);
            context.write(key, out); // ★ Combiner 출력도 LongPairWritable 유지
        }
    }

    /** Reducer: subreddit별 평균 score 계산 */
    public static class AvgReducer
            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {

        private final DoubleWritable avg = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            long sum = 0;
            for (LongPairWritable v : values) {
                count += v.get_0();
                sum += v.get_1();
            }
            if (count > 0) {
                avg.set((double) sum / count);
                context.write(key, avg);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RedditAverage <input> <output>");
            return 1;
        }

        Job job = Job.getInstance(getConf(), "reddit average score");
        job.setJarByClass(RedditAverage.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ExtractMapper.class);
        job.setCombinerClass(SumPairsCombiner.class); // Combiner 지정
        job.setReducerClass(AvgReducer.class);

	// Map 출력 타입 (Combiner 입출력 타입과 일치)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);

	// 최종 출력 타입 (Reducer 출력)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
}
