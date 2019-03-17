package ass2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class CreateNrFile {
    public static class MapperClass extends Mapper<LongWritable,Text,LongWritable,IntWritable> {
        private IntWritable one;
        private int col;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            one = new IntWritable(1);
            col = context.getConfiguration().getBoolean("col", true) ? 1 : 2;
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3){
                context.write(new LongWritable(Long.parseLong(splitted[col])),one);
            }
        }


        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<LongWritable,IntWritable,LongWritable, IntWritable> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(LongWritable r, Iterable<IntWritable> ones, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable one : ones){
                sum += one.get();
            }
            context.write(r, new IntWritable(sum));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<LongWritable,IntWritable> {

        @Override
        public int getPartition(LongWritable key, IntWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
