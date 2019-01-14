package org.apache.spark.ml.parity

import java.io.File

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.runtime.MleapSupport._
import com.databricks.spark.avro._
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{DataType, NodeShape, TensorType}
import ml.combust.mleap.runtime.frame.{BaseTransformer, MultiTransformer, SimpleTransformer}
import ml.combust.mleap.runtime.{MleapContext, frame}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.transformer.Pipeline
import org.apache.spark.ml.param.Param
import resource._

/**
  * Created by hollinwilkins on 10/30/16.
  */
object SparkParityBase extends FunSpec {
  val sparkRegistry = SparkBundleContext.defaultContext
  val mleapRegistry = MleapContext.defaultContext

  def textDataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.text(getClass.getClassLoader.getResource("datasources/carroll-alice.txt").toString).
      withColumnRenamed("value", "text")
  }

  def frTextDataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.text(getClass.getClassLoader.getResource("datasources/lyon.txt").toString).
      withColumnRenamed("value", "text")
  }
  def dataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.avro(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)
  }

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def recommendationDataset(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read.textFile(this.getClass.getClassLoader.getResource("datasources/sample_movielens_ratings.txt").toString)
                         .map(parseRating)
                         .toDF()
  }
}

abstract class SparkParityBase extends FunSpec with BeforeAndAfterAll {
  lazy val baseDataset: DataFrame = SparkParityBase.dataset(spark)
  lazy val textDataset: DataFrame = SparkParityBase.textDataset(spark)
  lazy val recommendationDataset: DataFrame = SparkParityBase.recommendationDataset(spark)
  lazy val frTextDataset: DataFrame = SparkParityBase.frTextDataset(spark)

  val dataset: DataFrame
  val sparkTransformer: Transformer

  lazy val spark = SparkSession.builder().
    appName("Spark/MLeap Parity Tests").
    master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.default.parallelism", "8")
    .config("spark.executor.cores", "2")
    .getOrCreate()


  override protected def afterAll(): Unit = spark.stop()

  var bundleCache: Option[File] = None

  def serializedModel(transformer: Transformer)
                     (implicit context: SparkBundleContext): File = {
    bundleCache.getOrElse {
      new File("/tmp/mleap/spark-parity").mkdirs()
      val file = new File(s"/tmp/mleap/spark-parity/${getClass.getName}.zip")
      file.delete()

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): frame.Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def deserializedSparkTransformer(transformer: Transformer)
                                  (implicit context: SparkBundleContext): Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadSparkBundle().get.root
    }).tried.get
  }

  def assertModelTypesMatchTransformerTypes(model: Model, shape: NodeShape, exec: UserDefinedFunction): Unit = {
    val modelInputTypes = shape.inputs.
      map(_._2.port).
      map(n => model.inputSchema.getField(n).get.dataType).
      toSeq
    val transformerInputTypes = exec.inputs.flatMap(_.dataTypes)

    val modelOutputTypes = shape.outputs.
      map(_._2.port).
      map(n => model.outputSchema.getField(n).get.dataType).
      toSeq
    val transformerOutputTypes = exec.outputTypes

    checkTypes(modelInputTypes, transformerInputTypes)
    checkTypes(modelOutputTypes, transformerOutputTypes)
  }

  def checkTypes(modelTypes: Seq[DataType], transformerTypes: Seq[DataType]): Unit = {
    assert(modelTypes.size == modelTypes.size)
    modelTypes.zip(transformerTypes).foreach {
      case (modelType, transformerType) => {
        if (modelType.isInstanceOf[TensorType]) {
          assert(transformerType.isInstanceOf[TensorType] &&
            modelType.base == transformerType.base)
        } else {
          assert(modelType == transformerType)
        }
      }
    }
  }

  def equalityTest(sparkDataset: DataFrame,
                   mleapDataset: DataFrame): Unit = {
    val sparkElems = sparkDataset.collect()
    val mleapElems = mleapDataset.collect()
    assert(sparkElems sameElements mleapElems)

  }

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {

      val sparkTransformed = sparkTransformer.transform(dataset)
      implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
      val mTransformer = mleapTransformer(sparkTransformer)
      val sparkDataset = sparkTransformed.toSparkLeapFrame.toSpark
      val mleapDataset = mTransformer.sparkTransform(dataset)
      equalityTest(sparkDataset, mleapDataset)
    }

    it("serializes/deserializes the Spark model properly") {
      val deserializedSparkModel = deserializedSparkTransformer(sparkTransformer)

      extractSparkTransformerParamsToVerify(deserializedSparkModel).foreach {
        case (param1, param2) =>
          assert(sparkTransformer.isDefined(param1) == deserializedSparkModel.isDefined(param2),
            s"spark transformer is define ${sparkTransformer.isDefined(param1)} deserialized is ${deserializedSparkModel.isDefined(param2)}")

          if(sparkTransformer.isDefined(param1)) {
            val v1Value = sparkTransformer.getOrDefault(param1)
            val v2Value = deserializedSparkModel.getOrDefault(param1)

            v1Value match {
              case v1Value: Array[_] =>
                assert(v1Value sameElements v2Value.asInstanceOf[Array[_]])
              case _ =>
                assert(v1Value == v2Value, s"$param1 is not equivalent")
            }
          }
      }
    }

    it("model input/output schema matches transformer UDF") {
      val mTransformer = mleapTransformer(sparkTransformer)

      mTransformer match {
        case transformer: SimpleTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
        case transformer: MultiTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
        case transformer: BaseTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.exec)
        case pipeline: Pipeline =>
          pipeline.transformers.foreach {
            case transformer: SimpleTransformer =>
              assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
            case transformer: MultiTransformer =>
              assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
            case stage: BaseTransformer =>
              assertModelTypesMatchTransformerTypes(stage.model, stage.shape, stage.exec)
            case _ => // no udf to check against
          }
        case _ => // no udf to check against
      }
   }
  }

  protected def extractSparkTransformerParamsToVerify(deserializedSparkModel: Transformer): Array[(Param[_], Param[_])] = {
    sparkTransformer.params.zip(deserializedSparkModel.params)
  }

  it should behave like parityTransformer()
}
