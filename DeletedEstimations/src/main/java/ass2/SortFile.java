package ass2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortFile {
    public static class MapperClass extends Mapper<LongWritable, Text, NgramProbComparable, IntWritable> {
        private IntWritable one;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            one = new IntWritable(1);
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            context.write(new NgramProbComparable(line),one);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<NgramProbComparable,IntWritable,NgramProbComparable,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(NgramProbComparable ngramProb, Iterable<IntWritable> ones, Context context)
                throws IOException,  InterruptedException {
            for (IntWritable one : ones){
                context.write(ngramProb, new Text(""));
            }

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<NgramProbComparable, IntWritable> {

        @Override
        public int getPartition(NgramProbComparable key, IntWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
