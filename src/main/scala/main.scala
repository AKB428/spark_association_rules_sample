import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset

object ArSample {
  def main(args: Array[String]): Unit = {

val conf = new SparkConf().setAppName("Ar Application")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

val freqItemsets = sc.parallelize(Seq(
  new FreqItemset(Array("a"), 15L),
  new FreqItemset(Array("b"), 35L),
  new FreqItemset(Array("a", "b"), 12L)
));

val ar = new AssociationRules()
  .setMinConfidence(0.8)
val results = ar.run(freqItemsets)

results.collect().foreach { rule =>
  println("[" + rule.antecedent.mkString(",")
    + "=>"
    + rule.consequent.mkString(",") + "]," + rule.confidence)
}

sc.exit
}
}
