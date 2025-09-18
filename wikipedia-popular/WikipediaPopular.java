import java.io.IOException;

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

public class WikipediaPopular extends Configured implements Tool {

    /** Mapper: 한 줄에서 timestamp, views 추출 -> (timestamp, views) */
    public static class ExtractMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>{

        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 한 줄 읽기
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // 공백 기준 split
            String[] fields = line.trim().split(" ");

            String timestamp = fields[0];   // 예: 20160801-020000
            String lang      = fields[1];   // 예: en
            String title     = fields[2];   // 예: Aaaah
            long views       = Long.parseLong(fields[3]); // 조회수

            // 조건: 영어(en) + Main_Page 제외 + Special: 시작 제외
            if (!"en".equals(lang)) return;
            if ("Main_Page".equals(title)) return;
            if (title.startsWith("Special:")) return;

            // Mapper 출력
            outKey.set(timestamp);
            outValue.set(views);
            context.write(outKey, outValue);
        }
    }

    /** Combiner: 같은 timestamp 당 가장 큰 views 정보만 남겨 셔플 트래픽 감소 */
    public static class MaxViewsCombiner
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable outValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long maxViews = Long.MIN_VALUE;

            for (LongWritable v : values) {
                maxViews = Math.max(maxViews, v.get());
            }

            outValue.set(maxViews);
            context.write(key, outValue);
        }
    }

    /** Reducer: timestamp 당 가장 큰 views 계산 */
    public static class MaxViewsReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable outValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long maxViews = Long.MIN_VALUE;

            for (LongWritable v : values) {
                maxViews = Math.max(maxViews, v.get());
            }

            outValue.set(maxViews);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wikipedia popular");
        job.setJarByClass(WikipediaPopular.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ExtractMapper.class);
        job.setCombinerClass(MaxViewsCombiner.class);
        job.setReducerClass(MaxViewsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
