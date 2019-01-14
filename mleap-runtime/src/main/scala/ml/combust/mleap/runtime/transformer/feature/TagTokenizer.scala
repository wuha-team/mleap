package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.TagTokenizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}

/**
  * Created by hwilkins on 12/30/15.
  */
case class TagTokenizer(override val uid: String = Transformer.uniqueName("tag_tokenizer"),
                     override val shape: NodeShape,
                     override val model: TagTokenizerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (text: String) => model(text)

}

