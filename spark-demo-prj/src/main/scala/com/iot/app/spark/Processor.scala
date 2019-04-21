package com.iot.app.spark

import java.util.Date
import com.iot.app.spark.entity.WindowTrafficeData
import com.iot.app.spark.entity.TotalTrafficeData
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import com.iot.data.producer.vo.IoTData
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.StringDecoder
import com.iot.app.spark.util.IoTDataDecoder
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.State
import java.lang.Boolean
import org.apache.spark.streaming.Minutes
import com.iot.app.spark.entity.POITraffic

object Processor {
  //case class TotalTrafficeData(routeId:String,vechileId:String,totalCount:Int,recordDate:String,timeStamp:Date)
 //case class POITraffic(vechileId:String,vechileType:String,distance:Double,timeStamp:Date)
  case class POIData(latitude:Double,longitute:Double,radius:Double)
  val tmp = new Date
  //var totalTrafficeDate= TotalTrafficeData("122", "1212", 1221, "12-01-2019",tmp)
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Iot app")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint("/Users/adarsh/Desktop/checkpoints-0")

    val topics = Set("iot-data-event")
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "spark-streaming-notes")

    val readStream = KafkaUtils.createDirectStream[String, IoTData, StringDecoder, IoTDataDecoder](ssc, 
        kafkaParams, topics)
   val nonFilterRecords= readStream.map(x => (x._2.getVehicleId,x._2)).reduce((a,b)=>a)
      .mapWithState(StateSpec.function(processVechilefunc _ ).timeout(Minutes(60)))
    val filterRecords=nonFilterRecords.filter(x=>x._2.equals(Boolean.FALSE)).map(x=>x._1)
    filterRecords.cache
    val poiData=POIData(33.877495,-95.50238,30)
    val poiDimentionBroadCast= sc.broadcast((poiData,"route-id","Truck"))
      //filterRecords.print()
     //TrafficProcess.totalTrafficProcess(filterRecords)
         TrafficProcess.windowTrafficProcess(filterRecords)   
    ssc.start()
    ssc.awaitTermination()
  }
  
  //process the passed vechiles
 def processVechilefunc(data:String,iot:Option[IoTData],state:State[Boolean]):(IoTData,Boolean)={
   var vechile=(iot.get,Boolean.FALSE)
   if(state.exists())
       vechile=(iot.get,Boolean.TRUE)
       else
         state.update(Boolean.TRUE)
       vechile
 }

}