package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructField}
import org.scalatest.FunSpec

/**
  * Created by hassanidk
  */
class TagTokenizerModelSpec extends FunSpec {

val tokenizer = TagTokenizerModel()
  describe("tokenizer") {

    it("returns the correct stem tokens from the given input string") {
      assert(tokenizer("hello there dude").sameElements(Array("helo", "ther", "dude")))
    }

    it("has the right input schema") {
      assert(tokenizer.inputSchema.fields == Seq(StructField("input", ScalarType.String.nonNullable)))
    }

    it("has the right output schema") {
      assert(tokenizer.outputSchema.fields == Seq(StructField("output", ListType(BasicType.String))))
    }


  }
}
