package SalesCountry;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            output.collect(t_key, new Text(values.next().toString()));
        }
    }
}
