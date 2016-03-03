package fr.ecp.sio;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private final static Splitter SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
    private final Text key = new Text();
    private final FloatWritable value = new FloatWritable();

    public TestMapper() {
        super();
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        /*  Mettre toutes les variables en final est une bonne pratique. Cela évite que le Garbage Collector tourne
            en arrière plan.
         */
        final List<String> tokens = Lists.newArrayList(SPLITTER.split(line.toString()));

        try {
            final Float temperature = Float.valueOf(tokens.get(9));
            key.set(tokens.get(0));
            value.set(temperature);
            context.write(key, value);
        } catch (NumberFormatException e) {
            context.getCounter("PARSING", "Temperature Error").increment(1);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // En principe il faudrait mettre ici la configuration :
        //context.getConfiguration().getInt("");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
