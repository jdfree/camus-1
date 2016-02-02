package com.linkedin.camus.etl.kafka.common;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AvroRecordWriterAndHivePartitionProviderTest {

    private AvroRecordWriterAndHivePartitionProvider provider;

    @Before
    public void setUp() throws Exception {
        provider = new AvroRecordWriterAndHivePartitionProvider(null);
    }

    public void testCreatePartition() throws Exception {

//        provider.createPartition(new Path("user/sourcedata/camus/camus_etl/raw_ad_activity/hourly/2016/01/32/00"));
    }
}