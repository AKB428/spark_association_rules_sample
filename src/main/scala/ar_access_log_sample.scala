import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset

object ArSample2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Ar Application")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("http://maru/page1"), 20L),
      new FreqItemset(Array("http://maru/page2"), 20L),
      new FreqItemset(Array("http://maru/page3"), 20L),
      new FreqItemset(Array("http://maru/page4"), 20L),
      new FreqItemset(Array("http://maru/page5"), 20L),
      new FreqItemset(Array("http://maru/page1", "http://maru/page2"), 19L),
      new FreqItemset(Array("http://maru/page4", "http://maru/page5"), 18L)
    ));

    val ar = new AssociationRules()
      .setMinConfidence(0.6)
    val results = ar.run(freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    }

    sc.stop
  }
}
