package es.udc.tpcxhs

import com.google.common.primitives.UnsignedBytes
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}


object HSSort extends java.io.Serializable {

  implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: flink run --class es.udc.tpcxhs.HSSort TPCx-HS-master_Flink.jar <INPUT_PATH> <OUTPUT_PATH>")
      return
    }

    // Get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    env.registerType(classOf[Array[Text]]) // Register type at Kryo

    // Parse arguments
    val inputPath = args(0)
    val outputPath = args(1)
    val partitions =  env.getParallelism

    // Job configuration
    val mapredConf = new JobConf()
    mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath)
    mapredConf.setInt("mapreduce.job.reduces", partitions)
    val jobContext = Job.getInstance(mapredConf)

    // Configure execution environment and execute
    env
      .createInput(HadoopInputs.readHadoopFile(new HadoopHSInputFormat(), classOf[Text], classOf[Text], inputPath))
      .partitionCustom(new HSSortPartitioner(partitions), 0)
      .sortPartition(0, Order.ASCENDING)
      .output(new HadoopOutputFormat[Text, Text](new HadoopHSOutputFormat(), jobContext))

    env.execute("HSSort")
  }
}
