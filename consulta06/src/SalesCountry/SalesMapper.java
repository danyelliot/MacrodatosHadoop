package SalesCountry;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 12) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        try {
            if (!data[11].trim().isEmpty()) {
                int pension65 = Integer.parseInt(data[11].trim());
                output.collect(new Text(departamento), new Text(String.valueOf(pension65)));
            }
        } catch (NumberFormatException e) {}
    }
}