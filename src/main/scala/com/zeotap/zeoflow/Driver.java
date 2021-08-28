package com.zeotap.zeoflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class Driver {

    static class Some{

    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        PCollection<Row> lines = p.apply(
                "ReadMyFile", TextIO.read().from("gs://some/inputData.txt"));

        PCollection<Row> rowPCollection = lines.apply(SqlTransform.query(""));


        rowPCollection.apply(TextIO.write().to(""));

        p.run().waitUntilFinish();
    }
}
