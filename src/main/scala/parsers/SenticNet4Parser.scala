package parsers

import pojo.{Polarity, SenticNet4Word}

import scala.xml.XML


class SenticNet4Parser {

  def unmarshallXml(): Seq[SenticNet4Word] = {
    val rdf = XML.loadFile("C:\\Users\\Alex\\Desktop\\senticnet4.rdf.xml")

    val senticNet4Words = (rdf \ "Description").map { desc =>
      val text = (desc \ "text").text
      val pleasantness = (desc \ "pleasantness").text.toFloat
      val attention = (desc \ "attention").text.toFloat
      val sensitivity = (desc \ "sensitivity").text.toFloat
      val aptitude = (desc \ "aptitude").text.toFloat
      //Result string = "#moodtag1#moodtag2"
      val moodtags = (desc \ "moodtag").text
      //Regex to split result mood string
      val pattern =
        """([#][^#]+)([#][^#]+)""".r
      val moodtagsList = pattern.findAllIn(moodtags).matchData.map(md => md.subgroups).flatten.toList
      val polarity = (desc \ "polarity").text match {
        case "positive" => Polarity.positive
        case "negative" => Polarity.negative
      }
      val intensity = (desc \ "intensity").text.toFloat
      //
      SenticNet4Word(text, pleasantness, attention, sensitivity, aptitude, moodtagsList, polarity, intensity)
    }
    senticNet4Words
  }
}
