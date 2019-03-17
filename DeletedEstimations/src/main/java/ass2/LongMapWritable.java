package ass2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Collection;
import java.util.Set;

public class LongMapWritable extends MapWritable {
    @Override
    public String toString(){
        Set<Writable> keys = this.keySet();
        Collection<Writable> values = this.values();
        long first = ((LongWritable)keys.iterator().next()).get(),
                second = ((LongWritable)values.iterator().next()).get();
        return first+"\t"+second;
    }
}
