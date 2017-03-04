
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

public class Model {

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
            if (count > t) {
                if (phrase.lastIndexOf(" ") > 0) {
                    outputPhrase.set(phrase.substring(phrase.lastIndexOf(" ") + 1) + ":" + count);
                    outputPrefix.set(phrase.substring(0, phrase.lastIndexOf(" ")));
                    context.write(outputPrefix, outputPhrase);  //output the prefix and current phrase with count
                }
                outputPhrase.set("[prefix]:" + count);
                outputPrefix.set(phrase);
                context.write(outputPrefix, outputPhrase);  //output itself as prefix and its count
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

            double denominator = 0;
            List<String> valueList = new LinkedList<String>();
            Map<String, Double> phraseMap = new HashMap<String, Double>();

            for (Text val : values) {    //for the same prefix
                String line = val.toString();
                String[] parts = line.split(":");
                String word = parts[0];
                double count = Double.parseDouble(parts[1]);
                if (word.equals("[prefix]")) {
                    denominator = count;  //get the denominator
                } else {
                    valueList.add(line);
                }

            }
            for (String oneValue : valueList) {
                String[] parts = oneValue.split(":");
                String word = parts[0];
                double count = Double.parseDouble(parts[1]);
                double probability = count / denominator;
                phraseMap.put(word.trim(), probability);   //get every probability
            }
            if (!phraseMap.isEmpty()) {
                List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(phraseMap.entrySet());
                Collections.sort(list, new ValueKeyComparator<String, Double>());
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
        job.setJarByClass(Model.class);
        job.setMapperClass(SortMapper.class);

        job.setReducerClass(ProbReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("model", ProbReducer.class, job);

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
