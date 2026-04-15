package SalesCountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        int n = 0;
        while (values.hasNext()) {
            String[] vals = values.next().toString().split(",");
            if (vals.length < 2) continue;
            try {
                double x = Double.parseDouble(vals[0].trim());
                double y = Double.parseDouble(vals[1].trim());
                sumX  += x;
                sumY  += y;
                sumXY += x * y;
                sumX2 += x * x;
                n++;
            } catch (NumberFormatException e) {}
        }
        if (n == 0) return;
        double pendiente  = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        double intercepto = (sumY - pendiente * sumX) / n;
        String resultado  = String.format("y = %.4f*x + %.4f  (n=%d distritos)", pendiente, intercepto, n);
        output.collect(t_key, new Text(resultado));
    }
}