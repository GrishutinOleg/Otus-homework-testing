import Task3.processTaxiDS
import structure_model.{TaxiRide, TaxiZone}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll




class GeneratedSparkTestSpec extends SharedSparkSession {

  import testImplicits._

  test("Pred test") {
    forAll { l: List[String] =>
      l.reverse.reverse shouldBe l
      println(l)
    }
  }

  test("Common generated test") {
    import DataGenerator._
    implicit val taxiZone: Arbitrary[TaxiZone]  = Arbitrary(genTaxiZone)
    implicit val taxiFacts: Arbitrary[TaxiRide] = Arbitrary(genTaxiFacts)

    forAll { (zones: Seq[TaxiZone], facts: Seq[TaxiRide]) =>
      val zonesDF = zones.toDF()
      val factsDF = facts.toDF()

      val actualResult = processTaxiDS(factsDF, zonesDF)

      actualResult.show()

      actualResult.foreach { r =>

        val sum_amount = r.get(2).asInstanceOf[Double]
        val avg_amount = r.get(3).asInstanceOf[Double]

        val avg_distance = r.get(4).asInstanceOf[Double]
        val min_distance = r.get(5).asInstanceOf[Double]
        val max_distance = r.get(6).asInstanceOf[Double]


        println(s"sum_amount:  ${sum_amount}")
        println(s"avg_amount:  ${avg_amount}")

        println(s"avg_distance:  ${avg_distance}")
        println(s"min_distance:  ${min_distance}")
        println(s"max_distance:  ${max_distance}")



        (min_distance <= avg_distance) shouldBe true
        (avg_distance <= max_distance) shouldBe true
        (avg_amount <= sum_amount) shouldBe true

        println(s"test OK")
      }
    }
  }



}
