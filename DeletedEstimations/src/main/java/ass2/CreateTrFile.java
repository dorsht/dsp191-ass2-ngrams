package ass2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CreateTrFile {
    public static class MapperClass extends Mapper<LongWritable, Text,LongWritable, LongWritable> {
        private int from, to;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            if (context.getConfiguration().getBoolean("direction",true)){
                from = 1;
                to = 2;
            }
            else{
                from = 2;
                to = 1;
            }
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3){
                context.write(
                        new LongWritable(Long.parseLong(splitted[from])),
                        new LongWritable(Long.parseLong(splitted[to])));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<LongWritable,LongWritable,LongWritable, LongWritable> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(LongWritable r, Iterable<LongWritable> otherRs, Context context) throws IOException,  InterruptedException {
            long count = 0;
            for (LongWritable otherR : otherRs){
                count += otherR.get();
            }
            context.write(r, new LongWritable(count));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    public static class PartitionerClass extends Partitioner<LongWritable,LongWritable> {

        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
