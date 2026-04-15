package SalesCountry;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<Double> lista = new ArrayList<>();
        while (values.hasNext()) {
            try { lista.add(Double.parseDouble(values.next().toString())); } catch (NumberFormatException e) {}
        }
        if (lista.isEmpty()) return;
        int n = lista.size();
        double suma = 0;
        for (double v : lista) suma += v;
        double promedio = suma / n;
        // Mediana
        Collections.sort(lista);
        double mediana = (n % 2 == 0) ? (lista.get(n/2 - 1) + lista.get(n/2)) / 2.0 : lista.get(n/2);
        // Desviacion estandar
        double sumaCuadrados = 0;
        for (double v : lista) sumaCuadrados += Math.pow(v - promedio, 2);
        double desviacion = Math.sqrt(sumaCuadrados / n);
        String resultado = String.format("promedio=%.2f mediana=%.2f desviacion=%.2f n=%d", promedio, mediana, desviacion, n);
        output.collect(t_key, new Text(resultado));
    }
}
