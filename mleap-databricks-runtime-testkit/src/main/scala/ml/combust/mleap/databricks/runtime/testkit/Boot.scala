package ml.combust.mleap.databricks.runtime.testkit

import org.apache.spark.sql.SparkSession

object Boot extends App {
  val session = SparkSession.builder().
    appName("TestMleapDatabricksRuntime")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      .config("spark.default.parallelism", "4")
      .config("spark.executor.cores", "2")
      .getOrCreate()
  val sqlContext = session.sqlContext

  new TestSparkMl(session).run()
  new TestTensorflow(session).run()
  new TestXgboost(session).run()

  session.close()
}
