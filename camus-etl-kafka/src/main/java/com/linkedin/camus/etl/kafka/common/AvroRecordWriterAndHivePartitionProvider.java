package com.linkedin.camus.etl.kafka.common;

import com.google.common.collect.Lists;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TException;

import java.io.IOException;

public class AvroRecordWriterAndHivePartitionProvider implements RecordWriterProvider {
  public final static String EXT = ".avro";
  private HiveMetaStoreClient hiveMetaStoreClient;

  public AvroRecordWriterAndHivePartitionProvider(TaskAttemptContext context) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,"thrift://mapr4d.devdfw2.lijit.com:9083");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,"300");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,"datasrv");
    try {
      hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getFilenameExtension() {
    return EXT;
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
                                                                 CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
    final DataFileWriter<Object> writer = new DataFileWriter<Object>(new SpecificDatumWriter<Object>());

    if (FileOutputFormat.getCompressOutput(context)) {
      if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
        writer.setCodec(CodecFactory.snappyCodec());
      } else {
        int level = EtlMultiOutputFormat.getEtlDeflateLevel(context);
        writer.setCodec(CodecFactory.deflateCodec(level));
      }
    }

    Path path = committer.getWorkPath();



    path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));
    FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

    createPartition(fileSystem.getWorkingDirectory().toString());


    writer.create(((GenericRecord) data.getRecord()).getSchema(),
            fileSystem
            .create(path));

    writer.setSyncInterval(EtlMultiOutputFormat.getEtlAvroWriterSyncInterval(context));

    return new RecordWriter<IEtlKey, CamusWrapper>() {
      @Override
      public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
        writer.append(data.getRecord());
      }

      @Override
      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }

  protected void createPartition(String path) {
    String[] camus_etls = path.split("/");
    String tableName;
    String dt;
    String hourlyPath;
    if (camus_etls.length >= 6) {
      tableName = camus_etls[4];
      dt = camus_etls[6] + camus_etls[7] + camus_etls[8] + camus_etls[9];
      hourlyPath = camus_etls[6] + "/" + camus_etls[7] + "/" + camus_etls[8] + "/" + camus_etls[9];
    } else {
      tableName = "peteTable";
      dt = "peteDt";
      hourlyPath = "badpath";

    }

      System.out.println("path.toString() = " + path.toString());
      System.out.println("tableName = " + tableName);
      System.out.println("dt = " + dt);
      System.out.println("hourlyPath = " + hourlyPath);
      Table table = null;
      Partition partition = null;
      try {

        table = hiveMetaStoreClient.getTable("sourcedata", tableName);


        partition = new Partition();
        partition.setTableName(tableName);
        partition.setDbName("sourcedata");
        partition.setValues(Lists.newArrayList(dt));
        partition.setSd(table.getSd());
        partition.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
        System.out.println("table.getSd().getSerdeInfo().getName() = " + table.getSd().getSerdeInfo().getName());
        System.out.println("original: partition.getSd().getLocation() = " + partition.getSd().getLocation());
        partition.getSd().setLocation(table.getSd().getLocation() + hourlyPath);
        System.out.println("changed to: partition.getSd().getLocation() = " + partition.getSd().getLocation());
        hiveMetaStoreClient.add_partition(partition);
      } catch (TException e) {
        try {
          throw new TException("path.toString() = " + path.toString()
                  + "\ntableName = " + tableName
                  + "\ndt = " + dt
                  + "\nhourlyPath = " + hourlyPath
                  + "\ntable.getSd().getSerdeInfo().getName() = " + (
                  table != null ? table.getSd().getSerdeInfo().getName() : "")
                  + "\npartition.getSd().getLocation() = " + (
                  partition != null ? partition.getSd().getLocation() : null)

          );
        } catch (Exception ex) {
          ex.printStackTrace();

        }
        e.printStackTrace();

      }
    }
}
