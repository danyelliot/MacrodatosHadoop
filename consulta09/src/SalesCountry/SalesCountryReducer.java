package SalesCountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long totalAfiliados = 0, totalAbonados = 0;
        while (values.hasNext()) {
            String[] vals = values.next().toString().split(",");
            try {
                totalAfiliados += Long.parseLong(vals[0].trim());
                totalAbonados  += Long.parseLong(vals[1].trim());
            } catch (Exception e) {}
        }
        output.collect(t_key, new Text(totalAfiliados + "," + totalAbonados));
    }
}

class SalesReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            output.collect(t_key, new Text(values.next().toString()));
        }
    }
}