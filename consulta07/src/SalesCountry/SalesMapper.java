package SalesCountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final String SUBTEXTO = "01";
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String linea = value.toString().trim();
        String[] data = linea.split(";");
        if (data.length < 2) return;
        String ubigeo = data[1].trim();
        if (ubigeo.contains(SUBTEXTO)) {
            output.collect(new Text(ubigeo), new Text(linea));
        }
    }
}
