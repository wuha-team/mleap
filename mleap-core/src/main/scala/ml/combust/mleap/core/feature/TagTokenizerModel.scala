package ml.combust.mleap.core.feature

import java.io.{IOException, StringReader}
import java.util
import java.util.regex.Pattern

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructType}
import org.apache.lucene.analysis.fr.{FrenchAnalyzer, FrenchLightStemFilter}
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.ElisionFilter
import org.apache.lucene.analysis.{Analyzer, CharArraySet, LowerCaseFilter, StopFilter, TokenStream}

/** Companion object for defaults.
  */

object WordTaggerAnalyzer extends Serializable {

  def createAnalyzer(article: String, coreNLP: Boolean): TokenStream = {
    val analyzer = new Analyzer() {
      override protected def createComponents(fieldName: String): Analyzer.TokenStreamComponents = { // TOKENIZER
        new Analyzer.TokenStreamComponents(new UAX29URLEmailTokenizer)
      }
    }

    val reader = getReader(article, coreNLP)
    val regex = "( |^)[l|m|t|qu|n|s|j|d|c|jusqu|quoiqu|lorsqu|puisqu]( |$)"
    val elision_with_spaces = new PatternReplaceCharFilter(Pattern.compile(regex), " ", reader)
    val input = analyzer.tokenStream("test", elision_with_spaces)
    val lowercase = new LowerCaseFilter(input)

    val with_or_without_accents = new ASCIIFoldingFilter(lowercase, false)
    val listCharacter = util.Arrays.asList("l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu")
    val charArray = new CharArraySet(listCharacter, true)
    val elision = new ElisionFilter(with_or_without_accents, charArray)
    val frenchAnalyzer = new FrenchAnalyzer
    val fr_stopwords = new StopFilter(elision, frenchAnalyzer.getStopwordSet)
    new FrenchLightStemFilter(fr_stopwords)

  }


  def getReader(article: String, coreNLP: Boolean): StringReader = {
    return new StringReader(article)
//    if (coreNLP) {
//      val article_pos = CoreNLP.taggedSentence(article)
//      val article_with_specific_pos = CoreNLP.keepPos(article_pos)
//      val article_without_pos = CoreNLP.removeTag(article_with_specific_pos)
//      new StringReader(article_without_pos)
//
//    } else {
//      new StringReader(article)
//    }
  }

  def analyse(article: String, coreNLP: Boolean = false): Seq[String] = {
    val stream = createAnalyzer(article, coreNLP)
    val result = new util.ArrayList[String]()
    try {
      stream.reset()
      val termAttribute = stream.addAttribute(classOf[CharTermAttribute])

      while (stream.incrementToken) {
        val currentWord = new String(termAttribute.buffer, 0, termAttribute.length)
        val regex = "[0-9]"
        if (!regex.r.findFirstIn(currentWord).isDefined) {
          result.add(currentWord)
        }
      }
    } catch {
      case ex: IOException =>
    }
    val array = new Array[String](0)
    result.toArray(array).toSeq
  }

}


class createTagTokenizerModel(vocab: Seq[String] =Seq[String](""), onlyVocabProvided: Boolean=false, coreNLP: Boolean=false) extends Serializable{
  val tokenizer = TagTokenizerModel(vocab, onlyVocabProvided, coreNLP)
}


/** Class for a custom tokenizer model.
  *
  * @param vocab vocabulary provided by user
  * @param onlyVocabProvided keep  only vocabulary provided by user
  * @param coreNLP only noun are kept if it's true
  */
case class TagTokenizerModel(vocab: Seq[String], onlyVocabProvided: Boolean, coreNLP: Boolean) extends Model {

  /**
    * Multiple regex to create custom features
    */
  val regexURL = "[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)"
  val regexMonth = "(janvier|fevrier|mars|avril|mai|juin|juillet|aout|août|septembre|octobre|novembre|decembre|" +
    "janv|fevr|avr|juill|sept|oct|nov|dec)"
  val regexDate = "([1-2][0-9] |[1-9] |3[0-1] )" + regexMonth + "|([0-9]){2}\\/([0-9]){2}\\/([0-9]){2}"
  val regexAddress = "[0-9]{1,3} (allee|avenue|boulevard|impasse|place|route|rue|chemin|bd)"
  val regexMail = "([a0-z-\\.-])*@([a-z-])*\\.[a-z]{2,3}"
  val regexYear = "(19|20)\\d{2}"
  val regexPeriodYear = regexYear + "(-| - | à )" + regexYear
  val regexMonthYear = regexMonth + " " + regexYear
  val regexPeriodMonthYear = regexMonthYear + "(-| - | à )" + regexMonthYear
  val regexCurency = "€|\\$"
  val regexPercent = "%"
  val regexPhoneNumber = "(0|\\\\+33|0033)[1-9][0-9 ]{12}|(0|\\\\+33|0033)[1-9][0-9]{8}|(0|\\\\+33|0033)[1-9][0-9\\.]{12}"
  val regexLineSeparator = " <WUHA_SKIP> "
  val regexTag = "#(?:END-)?WUHA-(?:BOLD|ITALIC|UNDERLINED|TITLE|SUBTITLE|LINK)#"
  val regexAge = "[0-9]{2} ans"
  val regexZipCode = " [0-9]{5} "

  // Max tokens processed
  val maxTokens = 700

  /** Tokenize a document string.
    *
    * Uses custom features to split the document into an array
    * @param document string to tokenize
    * @return array of tokens
    */
  def apply(document: String): Seq[String] = tokenize(document)

  override def inputSchema: StructType = StructType("input" -> ScalarType.String.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ListType(BasicType.String)).get

  /***
    *
    * @param tokens list of tokens
    * @return String from tokens
    */
  def extractStartAndEndOfText(tokens: Seq[String]): String = {
    if (tokens.length > maxTokens) {
      tokens.take(maxTokens).mkString(" ") + " " + tokens.takeRight(maxTokens).mkString(" ")
    } else{
      tokens.mkString(" ")
    }
  }

  /***
    *
    * @param text
    * @param longText
    * @return
    */
  def replaceSemanticsTokens(text: String, longText: Boolean): String = {
    val semanticsText = text.replaceAll(regexPeriodYear, " wuha_date_period ")
      .replaceAll(regexZipCode, " wuha_zip_code ")
      .replaceAll(regexAge, " wuha_age ")
      .replaceAll(regexPeriodMonthYear, " wuha_date_periode ")
      .replaceAll(regexDate, " wuha_date ")
      .replaceAll(regexMonthYear, " wuha_date ")
      .replaceAll(regexCurency, " wuha_currency ")
      .replaceAll(regexPercent, " wuha_percent ")
      .replaceAll(regexMail, " wuha_mail ")
      .replaceAll(regexURL, " wuha_url ")
      .replaceAll(regexAddress, " wuha_address ")
      .replaceAll(regexPhoneNumber, " wuha_phone_number ")
      .replaceAll(regexYear, " wuha_year ")

    if (longText) semanticsText + " wuha_long " else semanticsText

  }


  /**
    *
    * @param text String to be shorten
    * @return shortened text and true if the provided text is too long.
    *
    */
  def shortTextContent(text: String): (String, Boolean) = {
    val tokens = text.split(" ")
    val longText = if (tokens.length > maxTokens) true else false
    (extractStartAndEndOfText(tokens), longText)
  }

  def filterToken(token: String): Boolean = {
    if (onlyVocabProvided) vocab.contains(token) else true
  }


  /**
    *
    * @param text String with Wuha tags
    * @return String without tags
    */
  def removeTags(text: String): String = {
    val textWithoutTags = text.replaceAll(regexLineSeparator, " ")
    textWithoutTags.replaceAll(regexTag, "")
  }

  def tokenize(text: String, coreNLP: Boolean=false, checkTokensCV: Boolean=false): Seq[String] = {
    val (shorterText, longText) = shortTextContent(text)
    val textWithoutTags = removeTags(shorterText)
    val inputText = replaceSemanticsTokens(textWithoutTags.toLowerCase(), longText)

    val tokens = WordTaggerAnalyzer.analyse(inputText, coreNLP)
      .filter(x => x.length > 2 && filterToken(x))
    return tokens
  }


}

