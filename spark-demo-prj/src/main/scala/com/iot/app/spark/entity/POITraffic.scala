package com.iot.app.spark.entity

import java.util.Date
import scala.util.parsing.json.JSONFormat

case class POITraffic(vechileId:String,vechileType:String,distance:Double,timeStamp:Date)
case class WindowTrafficeData(routeId:String,vechileId:String,recordTime:String,timeStramp:Date)
case class TotalTrafficeData(routeId:String,vechileId:String,totalCount:Int,recordDate:String,timeStamp:Date)  
