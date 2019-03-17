package ass2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ass2.NCounter.Counter.N_COUNTER;

public class CreateSplitFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongMapWritable> {


        private static Pattern HEBREW_PATTERN = Pattern.compile("^[א-ת]+.*$|^\\d*\\s[א-ת]+.*$");
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            Text triplet = new Text();
            String[] splitted = line.toString().split("\t");
            Matcher m = HEBREW_PATTERN.matcher(splitted[0]);
            if (m.matches()){
                triplet.set(splitted[0]);
                LongMapWritable map = new LongMapWritable();
                LongWritable value = new LongWritable(Long.parseLong(splitted[2]));
                if (lineId.get() % 2 == 0) { // We want to split the file to 2 parts, such that most of the n-grams will
                    // appear in both of the parts
                    map.put(new LongWritable(0),value);
                }
                else {
                    map.put(new LongWritable(1),value);
                }
                context.write(triplet, map);
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

        public static class CombinerClass extends Reducer<Text,LongMapWritable,Text,LongMapWritable> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text line, Iterable<LongMapWritable> longMapWritables, Context context) throws IOException,  InterruptedException {
            long even = 0;
            long odd = 0;
            for (LongMapWritable longMapWritable : longMapWritables){
                LongWritable key = (LongWritable)longMapWritable.keySet().iterator().next();
                LongWritable value = (LongWritable)longMapWritable.values().iterator().next();
                if (key.get() == 0) {
                    even += value.get();
                }
                else {
                    odd += value.get();
                }
            }
            LongMapWritable oddMap = new LongMapWritable();
            LongMapWritable evenMap = new LongMapWritable();
            oddMap.put(new LongWritable(1L), new LongWritable(odd));
            evenMap.put(new LongWritable(0L), new LongWritable(even));

            context.write(line, oddMap);
            context.write(line, evenMap);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text, LongMapWritable,Text,LongMapWritable> {
        private long odd, even;
        private Counter N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getCounter(N_COUNTER);
        }

        @Override
        public void reduce(Text triplet, Iterable<LongMapWritable> occurrencesList, Context context) throws IOException,
                InterruptedException {
            even = 0;
            odd = 0;
            LongMapWritable occurrences = new LongMapWritable();
            for (LongMapWritable occurrence:occurrencesList) {
                LongWritable key = (LongWritable)occurrence.keySet().iterator().next();
                LongWritable value = (LongWritable)occurrence.values().iterator().next();
                if (key.get() == 0) {
                    even += value.get();
                }
                else {
                    odd += value.get();
                }
            }
            LongWritable oddLW = new LongWritable(odd), evenLW = new LongWritable(even);
            occurrences.put(evenLW, oddLW);
            context.write(triplet, occurrences);
            //long tempN = context.getConfiguration().getLong("N",1)+even+odd;
            //context.getConfiguration().setLong("N",tempN);
            N.increment(even+odd);
            even = 0;
            odd = 0;
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongMapWritable> {

        @Override
        public int getPartition(Text key, LongMapWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }
    }
}
