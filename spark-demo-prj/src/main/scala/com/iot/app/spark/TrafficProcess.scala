package com.iot.app.spark

import org.apache.spark.streaming.dstream.DStream
import com.iot.data.producer.vo.IoTData
import org.apache.spark.streaming.Seconds

object TrafficProcess {
  def totalTrafficProcess(filterRecords:DStream[IoTData]):Unit={
   val totalTraffice= filterRecords.map(x=>((x.getRouteId,x.getVehicleType),1)).reduceByKey(_+_)
    // totalTraffice.print()
     
  }
  def windowTrafficProcess(filterRecords:DStream[IoTData]){
    val windowTrafficDstream=filterRecords.map(x=>((x.getVehicleType,x.getRouteId),1)).
              reduceByKeyAndWindow(_+_,_+_, Seconds(30), Seconds(10))
          windowTrafficDstream.print()
  }
}