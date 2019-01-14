package org.apache.spark.ml.parity.feature

import ml.combust.mleap.runtime.tokenizer.TagTokenizer
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class TagTokenizerSpec extends SparkParityBase{
  override val dataset: DataFrame = textDataset.select("text")
    .withColumnRenamed("text", "textContents")
    .withColumn("label", lit(0))
  val tokenizer = new TagTokenizer(Seq[String]("chapt"), filterVocabulary = false)
    .setInputCol("textContents")
    .setOutputCol("tokens")

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(tokenizer)).fit(dataset)
}
