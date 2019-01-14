package org.apache.spark.ml.parity.feature

import ml.combust.mleap.runtime.tokenizer.TagTokenizer
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class TagTokenizerSpec extends SparkParityBase{
  override val dataset: DataFrame = textDataset.select("text")
    .withColumnRenamed("text", "textContent")
    .withColumn("label", lit(0))
  val tokenizer = new TagTokenizer(keepTokens = true)
    .setInputCol("textContent")
    .setOutputCol("tokens")

  val hashingTF = new HashingTF().setInputCol("tokens").setOutputCol("idf_features")
  val idf = new IDF().setInputCol("idf_features").setOutputCol("features")



  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(tokenizer, hashingTF, idf)).fit(dataset)



}
