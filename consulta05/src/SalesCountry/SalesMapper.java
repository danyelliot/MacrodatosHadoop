package SalesCountry;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String[] data = value.toString().trim().split(";");
        if (data.length < 5) return;
        String ubigeo = data[1].trim();
        if (ubigeo.length() < 2) return;
        String departamento = ubigeo.substring(0, 2);
        int afiliados = 0;
        try { if (!data[4].trim().isEmpty()) afiliados = Integer.parseInt(data[4].trim()); } catch (NumberFormatException e) {}
        output.collect(new Text(departamento), new IntWritable(afiliados));
    }
}