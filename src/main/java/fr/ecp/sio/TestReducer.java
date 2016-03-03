package fr.ecp.sio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public TestReducer() {
        super();
    }

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        long numberOfOccurences = 0;
        float total = 0.0f;
        for(FloatWritable value: values) {
            total += value.get();
            numberOfOccurences++;
        }

        Float average = total / numberOfOccurences;

        context.write(key, new FloatWritable(average));
    }
}
