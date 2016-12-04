package assignment4


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat


object Process {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("File Preprocessing")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration
    conf.set("textinputformat.record.delimiter", "/n/n")
    
    val dataset = sc.newAPIHadoopFile(args(0), classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
    val data = dataset.map { case (_, text) => text.toString}
    
    val reviews = data.map{r =>
      val fields = r.split("/n")
      ((fields(0).split("/t")(1),fields(3).split("/t")(1)),fields(6).split("/t")(1))
    }
    
    val output = args(1)
    reviews.saveAsTextFile(output)
    sc.stop()
    
  }
}