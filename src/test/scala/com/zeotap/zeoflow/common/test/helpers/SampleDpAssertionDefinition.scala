package com.zeotap.zeoflow.common.test.helpers

import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.column.helper.ColumnExpectationUtils.ColumnExpectationOps
import com.zeotap.zeoflow.common.test.helpers.DataPartnerExpectationUtils.wrapResult

class SampleDpAssertionDefinition {


  val minAgeExpectation: ColumnExpectation = ColumnExpectation("MinAge").addAgeExpectation().oneOf(List("18", "25", "35", "45", "55", "65"))
  val maxAgeExpectation: ColumnExpectation = ColumnExpectation("MaxAge").addAgeExpectation().oneOf(List("24", "34", "44", "54", "64", "99"))
  val interestIABExpectation: ColumnExpectation = ColumnExpectation("Interest_IAB_preprocess").addDynamicColumnExpectation()
  val genderExpectation: ColumnExpectation = ColumnExpectation("Gender").addGenderExpectation(List()).mayHaveNullLiteral
  val cityExpectation: ColumnExpectation = ColumnExpectation("city").mayHaveNullLiteral
  val createdTsExpectation: ColumnExpectation = ColumnExpectation("CREATED_TS_raw").hasValidLength(List(10)).addTimestampExpectation()
  val timestampExpectation: ColumnExpectation = ColumnExpectation("timestamp").hasValidLength(List(10)).addTimestampExpectation()

  val commonDataPartnerIdExpectation: ColumnExpectation = ColumnExpectation("Common_DataPartnerID").oneOf(List("1")).addMandatoryColumnExpectation()
  val deviceIdExpectation: ColumnExpectation = ColumnExpectation("DeviceId").oneOf(List("1","2","3","4")).mayHaveNullLiteral
  val demographicCountryINDExpectation: ColumnExpectation = ColumnExpectation("Demographic_Country").oneOf(List("IND")).addMandatoryColumnExpectation()
  val commonTSExpectation: ColumnExpectation = ColumnExpectation("Common_TS").hasValidLength(List(10)).addTimestampExpectation()
  val newColAddedThruUDFExpectation: ColumnExpectation = ColumnExpectation("newCol").oneOf(List("abc")).noneOf(List("xyz"))
  val newCol2AddedThruUDFExpectation: ColumnExpectation = ColumnExpectation("newCol2").oneOf(List("abc_sampleValue")).noneOf(List("abc"))

  val sampleDataPartner_preprocessDf_expectation_eu_profile: Array[ColumnExpectation] = Array(
    timestampExpectation,
    cityExpectation,
    interestIABExpectation,
    genderExpectation,
    minAgeExpectation,
    maxAgeExpectation,
    createdTsExpectation,
    ColumnExpectation("country_code_iso3").hasValidLength(List(3)).oneOf(List("FRA", "GBR", "ESP", "DEU")).addMandatoryColumnExpectation()
  )

  val sampleDataPartner_preprocessDf_expectation_in_profile: Array[ColumnExpectation] = Array(
    commonDataPartnerIdExpectation,
    deviceIdExpectation,
    commonTSExpectation,
    newColAddedThruUDFExpectation,
    newCol2AddedThruUDFExpectation,
    ColumnExpectation("Demographic_Country").oneOf(List("IND")).addMandatoryColumnExpectation()
  )

  def getColumnDSL(region: String, product: String): Map[String, ColumnDSL] = (region, product) match {
    case ("eu", "profile") => wrapResult[ColumnExpectation, ColumnDSL](List("preprocessDF"), List(sampleDataPartner_preprocessDf_expectation_eu_profile))
    case ("in", "profile") => wrapResult[ColumnExpectation, ColumnDSL](List("preprocessDF"), List(sampleDataPartner_preprocessDf_expectation_in_profile))
    case _ => Map("emptyDataset" -> ColumnDSL())
  }

}
