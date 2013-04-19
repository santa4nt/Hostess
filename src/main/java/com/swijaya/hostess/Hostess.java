package com.swijaya.hostess;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

public class Hostess {

    static class IpAddressValidator {

        private static final String IPV4_ADDRESS_PATTERN =
                "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
        private static final String IPV6_ADDRESS_PATTERN =
                "^([\\dA-F]{1,4}:|((?=.*(::))(?!.*\\3.+\\3))\\3?)" +
                "([\\dA-F]{1,4}(\\3|:\\b)|\\2){5}(([\\dA-F]{1,4}" +
                "(\\3|:\\b|$)|\\2){2}|(((2[0-4]|1\\d|[1-9])" +
                "?\\d|25[0-5])\\.?\\b){4})\\z";

        private final Pattern patternV4, patternV6;

        public IpAddressValidator() {
            patternV4 = Pattern.compile(IPV4_ADDRESS_PATTERN);
            patternV6 = Pattern.compile(IPV6_ADDRESS_PATTERN, Pattern.CASE_INSENSITIVE);
        }

        public boolean validate(String addr) {
            return patternV4.matcher(addr).matches() ||
                    patternV6.matcher(addr).matches();
        }

    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Log LOG = LogFactory.getLog(Map.class);

        private static final IpAddressValidator VALIDATOR = new IpAddressValidator();
        private static final IntWritable ONE = new IntWritable(1);

        private Text mapping = new Text();      // for output key: "[address]->[name]"

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");   // tokenize line with whitespace characters as delimiters

            if (tokens.length == 0)
                return;

            // examine the first token: if valid, it is either an address or comment character ('#')
            String first = tokens[0];
            if (first.equals(""))
                return;
            if (first.startsWith("#"))          // a comment line; skip
                return;
            if (!VALIDATOR.validate(first)) {   // not a valid address (could be an empty string)
                LOG.warn("Not a valid address: " + first);
                return;
            }
            if (tokens.length == 1) {           // this line only contains a (valid) address
                LOG.warn("Record on line " + key.toString() + " only contains an address");
                return;
            }
            if (tokens[1].startsWith("#")) {    // the next token is the start of a comment (no address, either)
                LOG.warn("Record on line " + key.toString() + " only contains an address (with inline comment)");
                return;
            }

            // for each host name entry in this line, emit a record:
            // { "[address]->[name]": 1 }
            for (int i = 1; i < tokens.length; i++) {
                String token = tokens[i];
                // the next token could be a comment character ('#'), in which case, ignore the rest of them
                if (token.startsWith("#"))
                    return;
                mapping.set(first + "->" + token);
                output.collect(mapping, ONE);
            }
        }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

        private Text address = new Text();
        private Text name = new Text();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                // decouple "[address]->[name]" mapping
                String[] tokens = key.toString().split("->");
                if (tokens.length != 2) {
                    assert false;
                    continue;
                }
                address.set(tokens[0]);
                name.set(tokens[1]);
                output.collect(address, name);
                // consume next record
                values.next();
            }
        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Hostess.class);
        conf.setJobName("hostess");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
