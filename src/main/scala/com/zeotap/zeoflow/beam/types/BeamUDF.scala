package com.zeotap.zeoflow.beam.types

import com.zeotap.zeoflow.common.types.FlowUDF
import org.apache.beam.sdk.transforms.SerializableFunction

final case class BeamUDF(name: String, function: SerializableFunction[_, _]) extends FlowUDF[BeamUDF] {
  override def register(): BeamUDF = BeamUDF(name, function)
}
