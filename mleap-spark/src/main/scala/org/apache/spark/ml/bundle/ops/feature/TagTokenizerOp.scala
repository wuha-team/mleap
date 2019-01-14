package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.runtime.tokenizer.TagTokenizer
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}


/**
  * Created by hollinwilkins on 8/21/16.
  */


class TagTokenizerOp extends SimpleSparkOp[TagTokenizer] {
  override val Model: OpModel[SparkBundleContext, TagTokenizer] = new OpModel[SparkBundleContext, TagTokenizer] {
    override val klazz: Class[TagTokenizer] = classOf[TagTokenizer]

    override def opName: String = Bundle.BuiltinOps.feature.tag_tokenizer

    override def store(model: Model, obj: TagTokenizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = { model }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): TagTokenizer = new TagTokenizer(Seq[String](""))
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: TagTokenizer): TagTokenizer = {
    new TagTokenizer(model.vocab, model.keepAllTokens, model.coreNLP)
  }

  override def sparkInputs(obj: TagTokenizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: TagTokenizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}