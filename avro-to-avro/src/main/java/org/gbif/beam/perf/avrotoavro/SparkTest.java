package org.gbif.beam.perf.avrotoavro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A vanilla spark API version to rewrite an Avro file.
 *
 * Example call:
 * <pre>
 *   spark2-submit --class org.gbif.beam.perf.avrotoavro.SparkTest --master yarn --executor-memory 20G \
 *     --executor-cores 5 --num-executors 20 avro-to-avro-0.1-SNAPSHOT.jar \
 *     /user/hive/warehouse/tim.db/occurrence_avro/* \
 *     /user/trobertson/sparktest
 * </pre>
 */
public class SparkTest {
  public static void main(String[] args) throws IOException {

    String input = args[0];
    String output = args[1];

    JavaSparkContext context = new JavaSparkContext();

    Schema schema = new Schema.Parser().parse(SparkTest.class.getResourceAsStream("occurrence-avro-schema.json"));
    context.hadoopConfiguration().set("avro.schema.output.key", schema.toString(false));

    // read
    JavaPairRDD<AvroKey, NullWritable> source =
        context.newAPIHadoopFile(
            input,
            AvroKeyInputFormat.class,
            AvroKey.class,
            NullWritable.class,
            new Configuration());

    // almost an identity function - modifies the ID to ensure no optimisations might come in to play
    JavaPairRDD<AvroKey, NullWritable> transform = source.mapToPair(new PairFunction<Tuple2<AvroKey,NullWritable>, AvroKey,NullWritable>() {
      @Override
      public Tuple2<AvroKey, NullWritable> call(Tuple2<AvroKey, NullWritable> v) throws Exception {
        GenericData.Record record = (GenericData.Record)v._1.datum();
        record.put("gbifid", ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
        return v;
      }
    });


    // write
    transform.saveAsNewAPIHadoopFile(
        output,
        AvroKey.class,
        NullWritable.class,
        AvroKeyOutputFormat.class);
  }
}
