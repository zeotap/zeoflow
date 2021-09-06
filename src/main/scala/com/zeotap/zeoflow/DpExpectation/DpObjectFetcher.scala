package com.zeotap.zeoflow.DpExpectation

class DpObjectFetcher {

  def dataPartnerAssertions(dpName: String): DataPartnerExpectation = {
    dpName match {
      case "delidatax" => new Delidatax
      case "kayak" => new Kayak
      case _ => new Kayak
    }
  }

}
