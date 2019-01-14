package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.TagTokenizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.TagTokenizer

/**
  * Created by hollinwilkins on 10/30/16.
  */
class TagTokenizerOp extends MleapOp[TagTokenizer, TagTokenizerModel] {
  override val Model: OpModel[MleapContext, TagTokenizerModel] = new OpModel[MleapContext, TagTokenizerModel] {
    override val klazz: Class[TagTokenizerModel] = classOf[TagTokenizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.tag_tokenizer

    override def store(model: Model, obj: TagTokenizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {

      model.withValue("vocab", Value.stringList(obj.vocab))
      model.withValue("onlyVocabProvided", Value.boolean(obj.onlyVocabProvided))
      model.withValue("coreNLP", Value.boolean(obj.coreNLP))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): TagTokenizerModel = {
      val vocab = model.value("vocab").getStringList
      val keepTokens = model.value("onlyVocabProvided").getBoolean
      val coreNLP = model.value("coreNLP").getBoolean
      TagTokenizerModel(vocab, keepTokens, coreNLP)

    }
  }

  override def model(node: TagTokenizer): TagTokenizerModel = node.model
}
