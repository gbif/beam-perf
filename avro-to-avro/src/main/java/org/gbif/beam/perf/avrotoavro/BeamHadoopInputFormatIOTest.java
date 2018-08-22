package org.gbif.beam.perf.avrotoavro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A rewrite on an avro file using Beam HadoopInputFormatIO.
 *
 * <p>Example call:
 *
 * <pre>
 *  spark2-submit --class org.gbif.beam.perf.avrotoavro.BeamHadoopInputFormatIOTest \
 *    --master yarn --executor-memory 16G --executor-cores 5 --num-executors 20 \
 *    avro-to-avro-0.1-SNAPSHOT-shaded.jar --runner=SparkRunner \
 *    --source=hdfs:///user/hive/warehouse/tim.db/occurrence_avro/* \
 *    --target=hdfs:///tmp/delme-beamio1
 * </pre>
 */
public class BeamHadoopInputFormatIOTest {
  public static void main(String[] args) throws IOException {
    AvroIOOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(AvroIOOptions.class);
    Pipeline p = Pipeline.create(options);

    Schema schema = new Schema.Parser().parse(SparkTest.class.getResourceAsStream("occurrence-avro-schema.json"));

    Configuration conf = new Configuration();
    conf.setClass("mapreduce.job.inputformat.class", AvroKeyInputFormat.class, InputFormat.class);
    conf.setClass("key.class", AvroKey.class, Object.class);
    conf.setClass("value.class", NullWritable.class, Object.class);
    conf.set("avro.schema.output.key", schema.toString(false));
    conf.set("mapreduce.input.fileinputformat.inputdir", options.getSource());

    p.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(schema));

    SimpleFunction<AvroKey, GenericRecord> keyTransform =
        new SimpleFunction<AvroKey, GenericRecord>() {
          public GenericRecord apply(AvroKey input) {
            return (GenericRecord) input.datum();
          }
        };

    PCollection<KV<GenericRecord, NullWritable>> source = p.apply("Read",
        HadoopInputFormatIO.<GenericRecord, NullWritable>read().withConfiguration(conf)
            .withKeyTranslation(keyTransform));

    PCollection<GenericRecord> transform = source.apply("Transform", ParDo.of(new DoFn<KV<GenericRecord, NullWritable>, GenericRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.element().getKey().put("gbifid", ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
        c.output(c.element().getKey());
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
