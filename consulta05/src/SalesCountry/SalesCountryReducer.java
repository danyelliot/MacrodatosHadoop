package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int mayor = Integer.MIN_VALUE;
        int menor = Integer.MAX_VALUE;
        while (values.hasNext()) {
            int val = ((IntWritable) values.next()).get();
            if (val > mayor) mayor = val;
            if (val < menor) menor = val;
        }
        output.collect(new Text(t_key + "_mayor"), new IntWritable(mayor));
        output.collect(new Text(t_key + "_menor"), new IntWritable(menor));
    }
}
