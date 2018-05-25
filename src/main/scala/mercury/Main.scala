package mercury

import com.opencsv.CSVReader
import java.nio.file.{ Files, Paths }
import java.time.{ Instant, Month, ZoneId, ZoneOffset, ZonedDateTime }
import java.io.Closeable

// Column Headers: Transaction ID, Financial Institution, Payment Type, Amount, Currency, Vendor, Tags, Date
// Example Rows:   19385281, "Wells Fargo", "Debit Card", "1.07", "USD", "STARBUCKS, INC.", "personal food", "2008-09-15Z15:53:00"
case class Line(
  id: String, // 0
  institution: String, // 1
  paymentType: String, // 2
  amount: Int, // 3, as cents b/c floats suck for money. Should probably be long.
  currency: String, // 4
  vendor: String, // 5
  tags: Seq[String], // 6
  date: Instant) // 7

object Line {
  def fromArray(l: Array[String]): Line = {
    new Line(l(0), l(1), l(2), (l(3).toDouble * 100).toInt, l(4), l(5), l(6).split(" "), Instant.parse(l(7)))
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val lines = readLines().map(Line.fromArray)

    // 1. How much was spent on the Wells Fargo Debit Card?
    val answer1 = lines
      .filter { l => l.institution == "Wells Fargo" && l.paymentType == "Debit Card" }
      .map { _.amount }
      .sum
    println("Spent on Wells Fargo Debit Card = " + answer1)

    // 2. How many unique vendors did the company transact with?
    val answer2 = lines.map(_.vendor).distinct.size
    println("Unique vendors = " + answer2)
    println("Unique vendors = " + lines.map(_.vendor).distinct)

    // 3. The company is worried too much money is being spent on perks for employees.
    // How much is being spent on items tagged either "personal" or "food"?
    val answer3 = lines.filter { l => l.tags.contains("food") || l.tags.contains("personal") }.map(_.amount).sum
    println("Amount on food or personal: " + answer3)

    // 4. The company had a party in London the weekend of January 23-25, 2012. How much did they spend over that period?
    // Tip: The time zone during this period is equivalent to UTC.
    val jan23 = ZonedDateTime.of(2012, 1, 23, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    val jan26 = ZonedDateTime.of(2012, 1, 26, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    val answer4 = lines.filter { l => l.date.isAfter(jan23) && l.date.isBefore(jan26) }.map(_.amount).sum
    println("London party = " + answer4)

    // 5. On how many evenings in December did the company buy from at least two distinct bars?
    // Only count purchases made in December between 6:00 PM to 12:00 AM Pacific Time.
    // The names of the bars are: "RICKHOUSE", "P.C.H.", "BLOODHOUND", "IRISH BANK".
    val pst = ZoneId.of("America/Los_Angeles")
    def isDecember(i: Instant) = i.atZone(pst).getMonth == Month.DECEMBER
    def isAtNight(i: Instant) = i.atZone(pst).getHour >= 18
    val bars = Seq("RICKHOUSE", "P.C.H.", "BLOODHOUND", "IRISH BANK")
    def isAtBar(l: Line) = bars.contains(l.vendor)
    def toDay(i: Instant) = i.atZone(pst).toLocalDate
    val answer5 = lines
      .filter { l => isDecember(l.date) && isAtNight(l.date) && isAtBar(l) }
      .map { l => (toDay(l.date), l.vendor) }
      .distinct
      .groupBy { t => t._1 }
      .filter { case (day, lines) => lines.length >= 2 }
      .size
    println("Days at two distinct bars " + answer5)

    // 6. Search the data for "suspicious spends" for each vendor.
    // Suspicious spends are defined as more than 1.75 standard deviations above the mean of purchases for a given vendor.
    // Print the vendor names and transaction IDs.
    def limit(amounts: Seq[Int]): Int = {
      // https://stackoverflow.com/questions/24192265/scala-finding-mean-and-standard-deviation-of-a-large-dataset
      val mean = amounts.sum / amounts.size
      val devs = amounts.map { a => (a - mean) * (a - mean) }
      val stdev = Math.sqrt(devs.sum / (amounts.size - 1))
      (mean + (1.75 * stdev)).toInt
    }
    val vendorLimits = lines
      .map { l => (l.vendor, l.amount) }
      .groupBy { t => t._1 } // vendor
      .mapValues { t => limit(t.map(_._2)) }
      .view.force // hack to de-view mapValues
    val answer6 = lines.filter { l => l.amount > vendorLimits(l.vendor) }
    answer6.groupBy(_.vendor).foreach {
      case (vendor, lines) => println(s"${vendor} had ${lines.size} suspicious transactions")
    }
  }

  private def readLines(): Seq[Array[String]] = {
    closing(Files.newBufferedReader(Paths.get("./csv_challenge.csv"))) { reader =>
      val csvReader = new CSVReader(reader)
      Iterator.continually(csvReader.readNext).drop(1).takeWhile(_ != null).toIndexedSeq
    }
  }

  /** Opens {@code resource} and ensures it is closed. */
  private def closing[T <: Closeable, R](resource: => T)(f: T => R): R = {
    val r = resource
    // Technically doing r.close in a finally can suppress the real exception
    // if r.close also throws, so the "best" way to do this is have finally
    // *also* do a try/catch. I'll go ahead and write it out...
    //
    // Turns out this is what the try-with-resources desugars to (which Java 8 has but Scala does not):
    // https://stackoverflow.com/questions/30934141/why-does-try-finally-block-not-register-the-original-exception-as-suppressed
    var original: Throwable = null;
    try {
      f(resource)
    } catch {
      case e: Throwable => original = e; throw e
    } finally {
      try {
        r.close()
      } catch {
        case e: Throwable if original == null => throw e
        case e: Throwable => original.addSuppressed(e); throw original
      }
    }
  }
}