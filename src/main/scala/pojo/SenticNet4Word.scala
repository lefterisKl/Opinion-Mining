package pojo

object Polarity extends Enumeration {
  val positive, negative = Value
}

case class SenticNet4Word(text: String, pleasantness: Float, attention: Float,
                          sensitivity: Float, aptitude: Float, moodTags: Seq[String],
                          polarity: Polarity.Value, intensity: Float) {
  //

  //Prints all fields and field values
  override def toString = s"SenticNet4Word(text=$text, pleasantness=$pleasantness, attention=$attention," +
    s" sensitivity=$sensitivity, aptitude=$aptitude, moodTags=$moodTags, polarity=$polarity, intensity=$intensity)"


  def canEqual(other: Any): Boolean = other.isInstanceOf[SenticNet4Word]

  //Objects are equal when text fields match
  override def equals(other: Any): Boolean = other match {
    case that: SenticNet4Word =>
      (that canEqual this) &&
        text == that.text
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(text)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
