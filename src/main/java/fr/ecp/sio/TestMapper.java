package fr.ecp.sio;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by erwanrouzel on 26/02/2016.
 */
public class TestMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    public TestMapper() {
        super();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);

        // Toute la logique du MapReduce est dans le map
        //
        // Les clés en sortie doivent être "Comparable", si ce n'est pas le cas c'est à moi de définir
        // un comparateur de clés
        //
        // On peut ne pas avoir de Reducer. Le Mapper est obligatoire mais pas le Reducer.

        context.write(key, value);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);

        // A overrider seulement si on sait ce qu'on fait.
        // => LE point d'entrée est généralement : map, setup et cleanup
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
