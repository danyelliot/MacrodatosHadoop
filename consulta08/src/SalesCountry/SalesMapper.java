package SalesCountry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private static final String FECHA_INICIO = "20241201";
    private static final String FECHA_FIN    = "20241231";
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        String linea = value.toString().trim();
        String[] data = linea.split(";");
        if (data.length < 2) return;
        String fecha = data[0].trim();
        if (fecha.compareTo(FECHA_INICIO) >= 0 && fecha.compareTo(FECHA_FIN) <= 0) {
            output.collect(new Text(fecha), new Text(linea));
        }
    }
}
