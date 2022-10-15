import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object YjcBigDataFinal_WeatherChange {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Former-USSR_Climate_Change")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //一个观测点数据导入
//    val pre_data1 = sc.textFile("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三下\\" +
//      "大数据技术基础\\ndp040\\ndp040\\f29263.dat")
    val pre_data1 = sc.textFile("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三下\\" +
      "大数据技术基础\\ndp040\\ndp040")
    //数据格式处理，将三个空格和两个空格都置换成一个空格，并切分数据。
    val fields = pre_data1.map(_.trim.replace("   ", " ")
      .replace("  ", " ").split(" "))
    //保留15个数据域的数据，并且符合QR为有观测数据和温度值不为999.9。
    //11位不能为9，,6\8\10位标志位不能为999，对数据用filter进行过滤
    val pre_ann = fields.filter(_.length == 15)
      .filter(_(9)!="999").filter(_(10)!="9")
    val pre_ann1 = fields.filter(_.length == 15).filter(_(2)=="8")
      .filter(_(5)!="999").filter(_(6)!="9")
    /**
     * 第一问：按照时间窗口统计单个观测点气温的变化趋势（以每日最高温度为依据），以月为单位
     */
//    val pre_ann_map = pre_ann.map(f=>((f(0),(f(1),f(2).toInt)),f(9).toFloat))
//    //对过滤数据进行分组压缩，再进行排序处理。
//    val High_Temp: RDD[((String, (String, Int)), Iterable[Float])] = pre_ann_map.groupByKey().sortByKey()
//    //rdd1.foreach(x=>{println(x)})
//    //求出每月最高气温的平均值
//    val High_Temp_mean: RDD[((String, (String, Int)), Float)] = High_Temp
    //    .map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
//    //pre_ann_mean1.foreach(x=>{println(x._1)})
//    High_Temp_mean.foreach(x=>{println(x)})
//    //pre_ann_mean1.saveAsTextFile("" + "./out")
    /**
     * 第二问：同一时间窗口内各个观测点的气温的变化趋势
     */
    val pre_ann_map1 = pre_ann1.map(f=>(((f(1),f(2).toInt),f(0)),f(5).toDouble))
    val Mean_Temp: RDD[(((String, Int), String), Iterable[Double])] = pre_ann_map1.groupByKey().sortByKey()
    val pre_ann_mean2: RDD[(((String, Int), String), Double)] = Mean_Temp
      .map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
    //val pre_ann_variance:RDD[(((String, Int), String), Iterable[Double])] = Mean_Temp.map(x=>{
    //  new Tuple2(x._1,x._2)})
//     val www = pre_ann_map1.union(pre_ann_mean2).groupByKey().map(x=>{
//      var sum=0.0
//      var num=0.0
//      var tem=0.0
//      var fang=0.0
//      for(i<-x._2){
//        num=num+1
//        if(num==x._2.size){
//          tem=i
//        }
//        for(i<-x._2){
//          sum=sum+(tem-i)*(tem-i)
//          }
//        fang=sum/num
//        (x._1,tem,fang)
//      }
//    })
//    www.foreach(x=>{println(x)})
    pre_ann_mean2.foreach(x=>{println(x)})
    //rdd_Union.foreach(x=>{println(x)})
//    pre_ann_variance.foreach(tuple2=>{
//      tuple2._2
//    })
  }
}
