package org.gbif.beam.perf.avrotoavro;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Description;

public interface AvroIOOptions extends HadoopFileSystemOptions {
  @Description("E.g. \"hdfs:///tmp/myavro/*\" ")
  String getSource();
  void setSource(String source);

  @Description("E.g. \"hdfs:///tmp/mytest\" ")
  String getTarget();
  void setTarget(String target);

}
