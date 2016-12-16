package parsers

import scala.xml.{NodeSeq, XML}


class SenticNet4Parser {
  //(xml \\ "Description" \ "text") text // pernei mono tin leksi

  def loadFile(): NodeSeq = {
    return XML.loadFile("C:\\Users\\Alexei\\Desktop\\singleWord.rdf.xml")
  }
}
