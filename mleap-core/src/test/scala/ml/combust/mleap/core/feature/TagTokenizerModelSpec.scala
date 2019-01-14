package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructField}
import org.scalatest.FunSpec

/**
  * Created by hassanidk
  */
class TagTokenizerModelSpec extends FunSpec {

val tokenizer = TagTokenizerModel(Seq[String](""), false, false)
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

    it("should keep only the start and end of text (700 first & last tokens)") {
      val text = List.range(1, 2000).mkString(" ")
      assert(tokenizer.shortTextContent(text)._1.split(" ").size == 1400)
    }

    it("should add semantics tokens") {
      val text = tokenizer.replaceSemanticsTokens("This text contains mail, address and year " +
        "wuha@contact.io 10 rue Laure Diebold, janvier 2013", false)
      assert(text.contains("wuha_mail") && text.contains("wuha_address") && text.contains("wuha_date") == true)
    }




  }
}
