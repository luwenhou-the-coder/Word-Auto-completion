
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

public class Bonus {

    //mapper
    public static class SortMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text outputPhrase = new Text();
        private Text outputPrefix = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int t = Integer.parseInt(conf.get("t"));
            String line = value.toString();
            String[] parts = line.split("\t");
            String phrase = parts[0];
            int count = Integer.parseInt(parts[1]);
            if (!phrase.contains(" ") && count > t) {
                for (int i = 1; i < phrase.length(); i++) {
                    outputPhrase.set(phrase + ":" + count);
                    outputPrefix.set(phrase.substring(0, i));
                    context.write(outputPrefix, outputPhrase);  //output the prefix and current phrase with count
                }
            }

        }
    }

    //reducer
    public static class ProbReducer
            extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));
            Map<String, Integer> phraseMap = new HashMap<String, Integer>();

            for (Text val : values) {    //for the same prefix
                String line = val.toString();
                String[] parts = line.split(":");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                phraseMap.put(word.trim(), count);  //directly put the word with its count into the map
            }
            if (!phraseMap.isEmpty()) {
                List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(phraseMap.entrySet());
                Collections.sort(list, new ValueKeyComparator<String, Integer>());
                int counter = 0;
                for (Map.Entry pair : list) {
                    counter++;
                    Put put = new Put(Bytes.toBytes(key.toString().trim()));//initialize Put
                    put.add(Bytes.toBytes("word"), Bytes.toBytes(pair.getKey().toString()), Bytes.toBytes(String.valueOf(pair.getValue())));
                    //add to column family word
                    context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
                    //output

                    if (counter >= n) { //only output top5
                        break;
                    }
                }
            }
        }

        public class ValueKeyComparator<K extends Comparable<? super K>, V extends Comparable<? super V>>
                implements Comparator<Map.Entry<K, V>> {

            public int compare(Map.Entry<K, V> a, Map.Entry<K, V> b) {
                int cmp1 = a.getValue().compareTo(b.getValue());
                if (cmp1 != 0) {
                    return -cmp1;
                } else {
                    return a.getKey().compareTo(b.getKey());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        if (args.length != 3) {
            System.err.println("Usage: wordcount <in> <t> <n>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Bonus.class);
        job.setMapperClass(SortMapper.class);

        job.setReducerClass(ProbReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("bonus", ProbReducer.class, job);

        //get parameters
        Options options = new Options();
        options.addOption("t", true, "Threshold");
        options.addOption("n", true, "N-grams");

        CommandLineParser basicparser = new BasicParser();

        CommandLine cmd = basicparser.parse(options, args);

        if (cmd.hasOption("t")) {
            conf.set("t", cmd.getOptionValue("t"));
        }
        if (cmd.hasOption("n")) {
            conf.set("n", cmd.getOptionValue("n"));
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
