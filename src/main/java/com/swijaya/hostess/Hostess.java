package com.swijaya.hostess;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
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

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Log LOG = LogFactory.getLog(Map.class);

        private static final IpAddressValidator VALIDATOR = new IpAddressValidator();
        private static final IntWritable ONE = new IntWritable(1);

        private Text mapping = new Text();      // for output key: "[address]->[name]"

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();  // strip first leading and trailing whitespaces
            String[] tokens = line.split("\\s+");   // tokenize line with whitespace characters as delimiters

            if (tokens.length == 0)
                return;

            // examine the first token: if valid, it is either an address or comment character ('#')
            String first = tokens[0];
            if (first.equals(""))               // this is the case with an empty line; skip
                return;
            if (first.startsWith("#"))          // a comment line; skip
                return;
            if (!VALIDATOR.validate(first)) {   // not a valid address (could be an empty string)
                LOG.warn(String.format("(file pos: %d) Not a valid address: %s", key.get(), first));
                return;
            }
            if (tokens.length == 1) {           // this line only contains a single (valid) address
                LOG.warn(String.format("(file pos: %d) Record only contains an address: %s", key.get(), first));
                return;
            }
            if (tokens[1].startsWith("#")) {    // the next token is the start of a comment (no address, either)
                LOG.warn(String.format("(file pos: %d) Record only contains an address (with inline comment): %s", key.get(), first));
                return;
            }

            // for each host name entry in this line, emit a record:
            // { "[address]->[name]": 1 }
            for (int i = 1; i < tokens.length; i++) {
                String token = tokens[i];
                // the next token could be a comment character ('#'), in which case, ignore the rest of them
                if (token.startsWith("#"))
                    break;
                mapping.set(first + "->" + token);
                context.write(mapping, ONE);
            }
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

        private Text address = new Text();
        private Text name = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // ignore values; multiple "ONE" values within the iterable means that there were duplicate records
            // from the input files, in which case we emit in the final output a single record
            String[] tokens = key.toString().split("->");   // decouple "[address]->[name]" mapping
            if (tokens.length != 2) {
                assert false;
                return;
            }
            address.set(tokens[0]);
            name.set(tokens[1]);
            context.write(address, name);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "Combine and flatten HOSTS files");

        job.setJarByClass(Hostess.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
