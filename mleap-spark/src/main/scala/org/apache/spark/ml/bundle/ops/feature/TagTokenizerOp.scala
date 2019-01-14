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
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("vocab", Value.stringList(obj.vocab))
        .withValue("onlyVocabProvided", Value.boolean(obj.onlyVocabProvided))
        .withValue("coreNLP", Value.boolean(obj.coreNLP))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): TagTokenizer = {
      val vocab = model.value("vocab").getStringList
      val keepTokens = model.value("onlyVocabProvided").getBoolean
      val coreNLP = model.value("coreNLP").getBoolean
      new TagTokenizer(vocab, keepTokens, coreNLP)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: TagTokenizer): TagTokenizer = {
    new TagTokenizer(model.vocab, model.onlyVocabProvided, model.coreNLP)
  }

  override def sparkInputs(obj: TagTokenizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: TagTokenizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}