package SalesCountry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 14) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        int ninos = 0;
        int iiee  = 0;
        try { if (!data[12].trim().isEmpty()) ninos = Integer.parseInt(data[12].trim()); } catch (NumberFormatException e) {}
        try { if (!data[13].trim().isEmpty()) iiee  = Integer.parseInt(data[13].trim()); } catch (NumberFormatException e) {}
        output.collect(new Text(departamento + "_ninos"), new IntWritable(ninos));
        output.collect(new Text(departamento + "_iiee"),  new IntWritable(iiee));
    }
}
