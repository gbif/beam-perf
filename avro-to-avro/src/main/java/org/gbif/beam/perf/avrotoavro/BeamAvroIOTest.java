package org.gbif.beam.perf.avrotoavro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.reflect.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A rewrite on an avro file using Beam AvroIO.
 *
 * <p>Example call:
 *
 * <pre>
 *  spark2-submit --class org.gbif.beam.perf.avrotoavro.BeamAvroIOTest \
 *    --master yarn --executor-memory 16G --executor-cores 5 --num-executors 20 \
 *    avro-to-avro-0.1-SNAPSHOT-shaded.jar --runner=SparkRunner \
 *    --source=hdfs:///user/hive/warehouse/tim.db/occurrence_avro/* \
 *    --target=hdfs:///tmp/delme-beamio1
 * </pre>
 */
public class BeamAvroIOTest {
  public static void main(String[] args) throws IOException {
    AvroIOOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(AvroIOOptions.class);
    Pipeline p = Pipeline.create(options);

    Schema schema = new Schema.Parser().parse(SparkTest.class.getResourceAsStream("occurrence-avro-schema.json"));

    PCollection<GenericRecord> source = p.apply("Read", AvroIO.readGenericRecords(schema).from(options.getSource()));

    // almost an identity function - modifies the ID to ensure no optimisations might come in to play
    PCollection<GenericRecord> transform = source.apply("Transform", ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.element().put("gbifid", ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
            c.output(c.element());
          }
        }));

    transform.apply("Write",
        AvroIO.writeGenericRecords(schema)
            .to(FileSystems.matchNewResource(options.getTarget(),true))
            // BEAM-2277 workaround
            .withTempDirectory(FileSystems.matchNewResource("hdfs://ha-nn/tmp/beam-avro", true)));

    p.run().waitUntilFinish();
  }
}
