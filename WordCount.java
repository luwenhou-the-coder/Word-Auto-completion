
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    //mapper
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            List<String> wordList = new LinkedList<String>();   //everytime initialize a linked list to store words
            String line = value.toString().toLowerCase();   //convert to lower case
            line = line.replaceAll("<ref.*?>|</ref.*?>|http.*? |https.*? |ftp.*? ", " ");
            //filter out <ref> and </ref>
            //filter out url
            line = line.replaceAll("[^a-z']", " "); //leave only words and '
            line = line.replaceAll("[^a-z]+'|'[^a-z]+|^'|'$", " "); //leave only apostrophe
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                wordList.add(itr.nextToken());
            }
            for (int i = 0; i < wordList.size(); i++) {
                String text;
                text = wordList.get(i);
                word.set(text);
                context.write(word, one);
                if (i < wordList.size() - 1) {
                    text += " " + wordList.get(i + 1);
                    word.set(text);
                    context.write(word, one);
                }
                if (i < wordList.size() - 2) {
                    text += " " + wordList.get(i + 2);
                    word.set(text);
                    context.write(word, one);
                }
                if (i < wordList.size() - 3) {
                    text += " " + wordList.get(i + 3);
                    word.set(text);
                    context.write(word, one);
                }
                if (i < wordList.size() - 4) {
                    text += " " + wordList.get(i + 4);
                    word.set(text);
                    context.write(word, one);
                }
            }
        }
    }

    //reducer

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
