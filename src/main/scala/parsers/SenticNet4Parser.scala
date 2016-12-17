package parsers

import pojo.SenticNet4Word

import scala.xml.XML


class SenticNet4Parser {

  def unmarshallXml(): Seq[SenticNet4Word] = {
    //TODO : prin to merge, vale to path san parametro h valto na to pernei apo http
    val rdf = XML.loadFile("C:\\Users\\Alexei\\Desktop\\singleWord.rdf.xml")

    val senticNet4Words = (rdf \ "Description").map { desc =>
      val text = (desc \ "text").text
      val pleasantness = (desc \ "pleasantness").text.toFloat
      val attention = (desc \ "attention").text.toFloat
      val sensitivity = (desc \ "sensitivity").text.toFloat
      val aptitude = (desc \ "aptitude").text.toFloat
      //Result string = "#moodtag1#moodtag2"
      val moodtags = (desc \ "moodtag").text
      //Regex to split result mood string
      val pattern ="""([#][^#]+)([#][^#]+)""".r
      val moodtagsList = pattern.findAllIn(moodtags).matchData.map(md => md.subgroups).flatten.toList

      //TODO : ftiakse parse string to enum
      val polarity = (desc \ "polarity").text
      val intensity = (desc \ "intensity").text.toFloat
      //
      SenticNet4Word(text, pleasantness, attention, sensitivity, aptitude, moodtagsList, null, intensity)
    }
    senticNet4Words
  }
}
