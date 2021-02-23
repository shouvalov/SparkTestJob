package shuvalov.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc, input_file_name, sum}
import org.apache.spark.sql.types.{BooleanType, DecimalType, IntegerType, StringType, StructType}

/**
 * По условиям задачи я понял, что данные по каждому сотруднику лежат в отдельных файлах. На вход программе передаются
 * пути к папкам с файлами.
 *
 * Т.к. в условиях задачи не описано, как должна вести себя программа если передан неверный путь к файлам с данными,
 * обработки ошибок на этот случай не сделано.
 *
 * В условиях задачи указано: "запишет в csv список всех работников c полями : name, salary, department". Неясно,
 * требуется ли записать данные в один файл или оставить это на усмотрение Spark. Для наглядности сделал запись в 1 файл
 * через coalesce(1).
 *
 *
 */
object TestSparkJob extends App {
  case class Employee(name: String, department: String, salary: String)

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("My great job")
//      .setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import org.apache.spark.sql.functions.udf
    val extractDepartment = udf(
      (path: String) => path.split("/").reverse.toList match {
        case _ /*fileName*/ :: department :: _ => department
        case _ => "???"
      })

    // - прочитает в параллель эти json
    val df =
      spark
        .read
        .option("multiLine", value = true)
        .json(args.map(path => s"$path/*.json"): _*)
        .withColumn("salary",col("salary").cast(IntegerType))
        .withColumn("department", extractDepartment(input_file_name()))
        .cache()

    // - запишет в csv список всех работников c полями : name, salary, department
    df
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("out")

//    df.printSchema
//    df.show()

    import spark.implicits._

    // - затем выведет на экран работника с самой большой зарплатой
    println(s"Most priced employee: ${
      df.orderBy(desc("salary")).head(1).toList.headOption.getOrElse("-")
    }")

    // - затем выведет на экран сумму зарплат всех работников
    println(s"Sum of salary: ${df.agg(sum("salary")).as[String].head()}")
  }
}
