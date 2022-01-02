package com.zeotap.data.io.helpers.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class BeamTestHelpers {

  public static ParDo.SingleOutput<GenericRecord, GenericRecord> printer = ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
      System.out.println(c.element().toString());
      c.output(c.element());
    }
  });

  public static class CubicIntegerFn implements SerializableFunction<String, Integer> {
    @Override
    public Integer apply(String input) {
      Integer intInput = Integer.parseInt(input);
      return intInput * intInput * intInput;
    }
  }

  public static CubicIntegerFn getCubicIntegerFn() {
    return new CubicIntegerFn();
  }

}
