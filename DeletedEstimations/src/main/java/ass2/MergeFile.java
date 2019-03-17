package ass2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeFile {
    public static class MapperClassNr extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 2){
                context.write(new Text (splitted[0]), new Text(splitted[1]+"\tNr"));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }
    public static class MapperClassTr extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 2){
                context.write(new Text (splitted[0]), new Text(splitted[1]+"\tTr"));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        private long N, Tr, Nr;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("N",1);
        }

        @Override
        public void reduce(Text ngram, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Tr = Nr = -1;
            for (Text value : values){
                String[] splitted = value.toString().split("\t");
                if (splitted.length == 2){
                    if (splitted[1].equals("Nr")){
                        Nr = Long.parseLong(splitted[0]);
                    }
                    else if (splitted[1].equals("Tr")){
                        Tr = Long.parseLong(splitted[0]);
                    }
                }
            }
            if (Tr != -1 && Nr != -1){
                double probability = 1.0 * (((Tr * 1.0) / N) / Nr);
                context.write(ngram, new DoubleWritable(probability));
            }

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
