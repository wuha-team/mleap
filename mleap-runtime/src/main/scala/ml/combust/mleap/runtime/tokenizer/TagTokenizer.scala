package ml.combust.mleap.runtime.tokenizer



import ml.combust.mleap.core.feature.createTagTokenizerModel
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}


class TagTokenizer(stemVocabulary: Seq[String]=Seq[String](""), filterVocabulary: Boolean=false, filterNoun: Boolean=false) extends Tokenizer  {

  override def setInputCol(value: String): this.type = set(inputCol, value)
  override def setOutputCol(value: String): this.type = set(outputCol, value)

  val vocab = stemVocabulary
  val onlyVocabProvided = filterVocabulary
  val coreNLP = filterNoun

  val tokenizerModel = new createTagTokenizerModel(vocab, onlyVocabProvided, coreNLP)


  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(${outputCol}, tokenizerUDF(col(${inputCol})))
  }


  def tokenizerUDF = udf ((textContents: String) =>  tokenizerModel.tokenizer.apply(textContents))


}
