package com.zeotap.zeoflow.DpExpectation

import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.column.helper.ColumnExpectationUtils.ColumnExpectationOps
import com.zeotap.utility.spark.generator.RandomDataGenerator
import com.zeotap.utility.spark.ops.DataColumnOps.DataColumnUtils
import com.zeotap.utility.spark.traits.{AlwaysPresent, DString}
import com.zeotap.utility.spark.types.DataColumn.dataColumn
import com.zeotap.utility.spark.types.{DataColumn, SparkDataframe}
import com.zeotap.zeoflow.DpExpectation.DpCommonUtils.wrapResult

class Kayak extends DataPartnerExpectation {

  val cookieArrayExpectation = ColumnExpectation("_cookieArray").mayHaveNullLiteral
  val minageExpectation = ColumnExpectation("MinAge").addAgeExpectation().oneOf(List("18", "25", "35", "45", "55", "65"))
  val maxageExpectation = ColumnExpectation("MaxAge").addAgeExpectation().oneOf(List("24", "34", "44", "54", "64", "99"))
  val interestIABExpectation = ColumnExpectation("Interest_IAB_preprocess").addDynamicColumnExpectation()
  val genderExpectation = ColumnExpectation("Gender").addGenderExpectation(List()).mayHaveNullLiteral
  val telconameExpectation = ColumnExpectation("telconame").mayHaveNullLiteral
  val cityExpectation = ColumnExpectation("city").mayHaveNullLiteral
  val id_mid_10Expectation = ColumnExpectation("id_mid_10_raw").isNonNull.hasValidLength(List(36))
  val createdTsExpectation = ColumnExpectation("CREATED_TS_raw").hasValidLength(List(10)).addTimestampExpectation()
  val timestampExpectation = ColumnExpectation("timestamp").hasValidLength(List(10)).addTimestampExpectation()

  val timestampDatacolumn = dataColumn("timestamp", DString, AlwaysPresent, List("2021-08-11", "2021-07-30", "2020-05-13"))
  val id_mid_10_rawDatacolumn = dataColumn("id_mid_10_raw", DString, AlwaysPresent, RandomDataGenerator.UUID(5))
  val cityDatacolumn = dataColumn("city", DString, AlwaysPresent, List()).withNull
  val telconameDatacolumn = dataColumn("telconame", DString, AlwaysPresent, List()).withNull
  val genderDatacolumn = dataColumn("Gender", DString, AlwaysPresent, List("Female", "Male")).withNull
  val minageDatacolumn = dataColumn("MinAge", DString, AlwaysPresent, List("35", "55", "18", "25", "45", "65")).withNull
  val maxageDatacolumn = dataColumn("MaxAge", DString, AlwaysPresent, List("44", "64", "24", "34", "54", "99")).withNull
  val createdTsDatacolumn = dataColumn("CREATED_TS_raw", DString, AlwaysPresent, List("1628683800", "1627409640", "1628623500"))
  val interestIABDataColumn = dataColumn("Interest_IAB_preprocess", DString, AlwaysPresent, List("", "IAB6", "IAB1_2", "IAB6,IAB1_2", "IAB1_2,IAB19"))


  val delidatax_preprocessdf_expectation_eu_profile = Array(
    cookieArrayExpectation,
    timestampExpectation,
    id_mid_10Expectation,
    cityExpectation,
    telconameExpectation,
    interestIABExpectation,
    genderExpectation,
    minageExpectation,
    maxageExpectation,
    createdTsExpectation,
    ColumnExpectation("country_code_iso3").hasValidLength(List(3)).oneOf(List("FRA", "GBR", "ESP", "DEU")).addMandatoryColumnExpectation()
  )

  val delidatax_preprocessdf_expectation_in_profile = Array(
    cookieArrayExpectation,
    timestampExpectation,
    id_mid_10Expectation,
    cityExpectation,
    telconameExpectation,
    interestIABExpectation,
    genderExpectation,
    minageExpectation,
    maxageExpectation,
    createdTsExpectation,
    ColumnExpectation("country_code_iso3").hasValidLength(List(3)).oneOf(List("IND")).addMandatoryColumnExpectation()
  )

  val delidatax_preprocessdf_expectation_us_profile = Array(
    cookieArrayExpectation,
    timestampExpectation,
    id_mid_10Expectation,
    cityExpectation,
    telconameExpectation,
    interestIABExpectation,
    genderExpectation,
    minageExpectation,
    maxageExpectation,
    ColumnExpectation("country_code_iso3").hasValidLength(List(3)).oneOf(List("MEX", "CAN", "COL", "BRA")).addMandatoryColumnExpectation()
  )

  val delidatax_preprocessdf_in_profile = Array(
    timestampDatacolumn,
    id_mid_10_rawDatacolumn,
    cityDatacolumn,
    telconameDatacolumn,
    interestIABDataColumn,
    genderDatacolumn,
    minageDatacolumn,
    maxageDatacolumn,
    createdTsDatacolumn,
    dataColumn("country_code_iso3", DString, AlwaysPresent, List("IND"))
  )

  val delidatax_preprocessdf_eu_profile = Array(
    timestampDatacolumn,
    id_mid_10_rawDatacolumn,
    cityDatacolumn,
    telconameDatacolumn,
    interestIABDataColumn,
    genderDatacolumn,
    minageDatacolumn,
    maxageDatacolumn,
    createdTsDatacolumn,
    dataColumn("country_code_iso3", DString, AlwaysPresent, List("FRA", "GBR", "ESP", "DEU"))
  )

  val delidatax_preprocessdf_us_profile = Array(
    id_mid_10_rawDatacolumn,
    interestIABDataColumn,
    genderDatacolumn,
    minageDatacolumn,
    maxageDatacolumn,
    dataColumn("city", DString, AlwaysPresent, List("Santiago de Cali", "Tijuana", "Puebla City", "Mexico City")).withNull,
    dataColumn("country_code_iso3", DString, AlwaysPresent, List("MEX", "BRA", "COL", "CAN", "USA")),
    dataColumn("telconame", DString, AlwaysPresent, List("SUMICITY TELECOMUNICACOES S.A.", "SNET-FCC", "FIBRESTREAM")).withNull,
    dataColumn("timestamp", DString, AlwaysPresent, List("2021-01-06", "2021-01-06 06:37:10 UTC", "2021-01-06 02:04:21 UTC"))
  )


  override def getColumnDSL(region: String, product: String): Map[String, ColumnDSL] = (region, product) match {
    case ("eu", "profile") => wrapResult[ColumnExpectation, ColumnDSL](List("preprocessDF"), List(delidatax_preprocessdf_expectation_eu_profile))
    case ("in", "profile") => wrapResult[ColumnExpectation, ColumnDSL](List("preprocessDF"), List(delidatax_preprocessdf_expectation_in_profile))
    case ("us", "profile") => wrapResult[ColumnExpectation, ColumnDSL](List("preprocessDF"), List(delidatax_preprocessdf_expectation_us_profile))
    case _ => Map("emptyDataset" -> ColumnDSL())
  }

  override def getRawDataDefinition(region: String, product: String): Map[String, SparkDataframe] = (region, product) match {
    case ("eu", "profile") => wrapResult[DataColumn, SparkDataframe](List("preprocessDF"), List(delidatax_preprocessdf_eu_profile))
    case ("in", "profile") => wrapResult[DataColumn, SparkDataframe](List("preprocessDF"), List(delidatax_preprocessdf_in_profile))
    case ("us", "profile") => wrapResult[DataColumn, SparkDataframe](List("preprocessDF"), List(delidatax_preprocessdf_us_profile))
    case _ => Map("emptyDataset" -> SparkDataframe())
  }

}
