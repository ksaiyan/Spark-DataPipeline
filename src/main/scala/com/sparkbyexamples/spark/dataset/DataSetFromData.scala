package com.sparkbyexamples.spark.dataset

import org.apache.spark.sql.SparkSession

object DataSetFromData {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val data = Seq((1,2),(3,4),(5,6))
  }
}







def createLoyaltyMemberTierAggDF(memberTierDS: Dataset[LoyaltyMemberTierRecord], tpidDs: Dataset[TpidRecord]): DataFrame = {

  val windowSpec = Window.partitionBy(
  LoyaltyMemberTierRecord.trvl_acct_id,
  LoyaltyMemberTierRecord.tpid,
  LoyaltyMemberTierRecord.loylty_prog_id,
  LoyaltyMemberTierRecord.loylty_membr_id
  ).orderBy(desc(LoyaltyMemberTierRecord.loylty_tier_eff_start_date))

  //Code logic for deriving last onekey cash brand for a given user

  val windowSpecLastOKCBrand = Window.partitionBy("trvl_acct_id").orderBy(col("updated_datetm").desc)

  val lastOneKeyCashMember = memberTierDS.filter(col(LoyaltyMemberTierRecord.loylty_tier_rnk_nbr).isNotNull)
  .filter(!col(LoyaltyMemberTierRecord.loylty_prog_id).isin(INVALID_LOYALTY_PROG_ID_VALUES: _*))
  .filter(col(LoyaltyMemberTierRecord.loylty_tier_desc).isin(LOYALTY_TIERS: _*))
  .filter(col(LoyaltyMemberTierRecord.trvl_acct_id).isNotNull)
  .filter(col(LoyaltyMemberTierRecord.trvl_acct_id_actv_flag) === "Y")
  .filter(col(LoyaltyMemberTierRecord.tpid).isNotNull)
  .filter(col(LoyaltyMemberTierRecord.updated_datetm).isNotNull)
  .filter(!col(LoyaltyMemberTierRecord.tpid).isin(INVALID_TPID_VALUES: _*))
  .select(
  col(LoyaltyMemberTierRecord.tpid),
  col(LoyaltyMemberTierRecord.trvl_acct_id),
  col(LoyaltyMemberTierRecord.updated_datetm)
  )

  val oneKeyRankedMember = lastOneKeyCashMember
  .withColumn("rank", rank().over(windowSpecLastOKCBrand))
  .filter(col("rank") === 1)
  .select("updated_datetm", "trvl_acct_id", "tpid")

  val tpidDim = tpidDs
  .filter(col(TpidRecord.tpid).isNotNull && !col(TpidRecord.tpid).isin(INVALID_TPID_VALUES: _*))
  .withColumnRenamed("tpid_name", "last_earned_onekey_cash_brand")
  .select(col("tpid"), col("last_earned_onekey_cash_brand"))

  val lastOneKeyCashBrand = oneKeyRankedMember
  .join(tpidDim, Seq("tpid"), "inner")
  .select(col("last_earned_onekey_cash_brand"), col("trvl_acct_id"))

  val preLoyaltyMemberTierAggDF = memberTierDS
  .filter(col(LoyaltyMemberTierRecord.loylty_tier_rnk_nbr).isNotNull)
  .filter(!col(LoyaltyMemberTierRecord.loylty_prog_id).isin(INVALID_LOYALTY_PROG_ID_VALUES: _*))
  .filter(col(LoyaltyMemberTierRecord.loylty_tier_desc).isin(LOYALTY_TIERS: _*))
  .filter(col(LoyaltyMemberTierRecord.trvl_acct_id).isNotNull)
  .filter(col(LoyaltyMemberTierRecord.trvl_acct_id_actv_flag) === "Y")
  .filter(col(LoyaltyMemberTierRecord.tpid).isNotNull)
  .filter(col(LoyaltyMemberTierRecord.updated_datetm).isNotNull)
  .filter(!col(LoyaltyMemberTierRecord.tpid).isin(INVALID_TPID_VALUES: _*))
  .select(
  col(LoyaltyMemberTierRecord.tpid),
  col(LoyaltyMemberTierRecord.loylty_membr_id).cast(LongType).as(LoyaltyMemberTierRecord.loylty_membr_id),
  col(LoyaltyMemberTierRecord.loylty_tier_rnk_nbr),
  col(LoyaltyMemberTierRecord.loylty_prog_id),
  col(LoyaltyMemberTierRecord.loylty_tier_desc),
  col(LoyaltyMemberTierRecord.trvl_acct_id),
  col(LoyaltyMemberTierRecord.trvl_acct_id_actv_flag),
  col(LoyaltyMemberTierRecord.updated_datetm),
  row_number.over(windowSpec).as(ROW_NUM))
  .filter(col(ROW_NUM) === 1)
  .drop(ROW_NUM)

  val loyaltyMemberTierAggDF = Old_Data.join(new_Data, Seq("Name"), "left")

  Old_Data (1bn records):
  Name  Phone
  Muskan 9888583677
  Kapil 9560558085
  Romeo -9999 null

  Muskan 9888583677
  Kapil 9560558085
  Romeo -9560558085 null

  Muskan 9888583677
  Kapil 9560558085
  Romeo -9999 null


  new_Data(1mn records):
  Muskan, "Muskan is okay",
  Kapil, "Kapil is Awesome"

  Muskan, "Muskan is okay",
  Kapil, "Kapil is Awesome"

  Muskan, "Muskan is okay",
  Kapil, "Kapil is Awesome"


  loyaltyMemberTierAggDF
  }