package SalesCountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

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
                double pension65 = Double.parseDouble(data[11].trim());
                double x = Double.parseDouble(ubigeo.substring(2));
                output.collect(new Text(departamento), new Text(x + "," + pension65));
            }
        } catch (NumberFormatException e) {}
    }
}
