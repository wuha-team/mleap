package ml.combust.mleap.runtime.tokenizer



import ml.combust.mleap.core.feature.TagTokenizerModel
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}


class TagTokenizer(listVocab: Seq[String]=Seq[String](""), keepTokens: Boolean=true, useCoreNLP: Boolean=false) extends Tokenizer  {

  override def setInputCol(value: String): this.type = set(inputCol, value)
  override def setOutputCol(value: String): this.type = set(outputCol, value)


  val vocab = listVocab
  val keepAllTokens = keepTokens
  val coreNLP = useCoreNLP


  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(${outputCol}, tokenizerUDF(col(${inputCol})))
  }


  def tokenizerUDF = udf ((textContents: String) =>  TagTokenizerModel.defaultTokenizer.apply(textContents))


}
