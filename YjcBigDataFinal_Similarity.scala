import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object YjcBigDataFinal_Similarity {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("Similarity")
    val sc = new SparkContext(conf)
    /**
     * 导入文件
     */
    val lines1: RDD[String] = sc.textFile("./A Tale of Two Cities - Charles Dickens.txt")
    val lines2: RDD[String] = sc.textFile("./David Copperfield - Charles Dickens.txt")
    /**
     * 进行排除标点符号操作和分词
     */
    val words1: RDD[String] = lines1.flatMap(line=>{
      line.replace(","," ")
        .replace("."," ")
        .replace(";"," ")
        .replace(":"," ")
        .replace("`"," ")
        .replace("?"," ")
        .replace("!"," ")
        .replace("-"," ")
        .replace("("," ")
        .replace(")"," ")
        .replace('"',' ')
        .replace("'"," ")
        .replace("  "," ")
        .split(" ")
    })
    val words2: RDD[String] = lines2.flatMap(line=>{
      line.replace(","," ")
        .replace("."," ")
        .replace(";"," ")
        .replace(":"," ")
        .replace("`"," ")
        .replace("?"," ")
        .replace("!"," ")
        .replace("-"," ")
        .replace("("," ")
        .replace(")"," ")
        .replace('"',' ')
        .replace("'"," ")
        .replace("  "," ")
        .split(" ")
    })
    val pairWords1:RDD[(String,Int)] = words1.map(word=>{
      new Tuple2(word,1)
    })
    val pairWords2:RDD[(String,Int)] = words2.map(word=>{
      new Tuple2(word,1)
    })
    /**
     * reduceByKey 先分组，后对每一组内的Key对应的value去聚合,
     * reduceByKey的作用就是对相同key的数据进行处理，最终每个key只保留一条记录
     */
    val result1 = pairWords1.reduceByKey((v1:Int,v2:Int)=>{v1+v2}).collect() //RDD不能嵌套，所以要转成数组形式
    val result2 = pairWords2.reduceByKey((v1:Int,v2:Int)=>{v1+v2}).collect()
    val result3 = pairWords1.reduceByKey((v1:Int,v2:Int)=>{v1+v2}) //RDD取交并集需要RDD形式
    val result4 = pairWords2.reduceByKey((v1:Int,v2:Int)=>{v1+v2})
    /**
     * 方法1：杰卡德相似度方法
     * 交并集之比，表示文本的元素差异度
     */
    val rdd_Intersection = result3.intersection(result4).collect //两个RDD的交集
    val rdd_Union = result3.union(result4).collect //两个RDD的并集
    var Length_Intersection = 0.0
    var Length_Union = 0.0
    rdd_Intersection.foreach(tuple=>{
      Length_Intersection += 1.0
    })
    rdd_Union.foreach(tuple=>{
      Length_Union += 1.0
    })
    println("方法1：杰卡德方法计算文本相似度为："+"\n" +
      ""+(Length_Intersection/Length_Union)*100.0.toFloat+"%")

    /**
     * 方法2：余弦距离法计算文本相似度
     * 分子只用算重合单词的词频乘积，
     * 因为不重合的部分分子乘积为0*N（N为不重合的随机词频）的形式，所以可以直接省略。
     */
    var x = 0
    var y = 0
    var z = 0
    result1.foreach(tuple1=> {
      result2.foreach(tuple2 => {
        if (tuple1._1 == tuple2._1) {
          x += tuple1._2*tuple2._2
        }
      })
    })
    result1.foreach(tuple1=>{
      y += tuple1._2*tuple1._2
    })
    result2.foreach(tuple2=>{
      z += tuple2._2*tuple2._2
    })
    val cos = (x/(math.sqrt(y)*math.sqrt(z))).toFloat //求出余弦值
    val similarity = (1.0-cos)*100.0
    println("方法2：余弦距离算法文本相似度为：" +"\n"+ similarity + "%")
    /**
     * 方法3：特征向量归一化的余弦算法
     * 考虑到文本数据量过大，惯用的余弦方法分母亿级开根号可能会造成很大的误差，
     * 所以采用归一化的的思想，取倒数求和，假设趋于无穷时和为一，对于亿级数据的根号误差有着良好的缓和作用
     * 弊端：易受极端值影响
     */
    var a=0.0
    var b=0.0
    var c=0.0
    result1.foreach(tuple1=> {
      result2.foreach(tuple2 => {
        if (tuple1._1 == tuple2._1) {
          a += (1/(tuple1._2*tuple2._2)).toFloat
        }
      })
    })
    result1.foreach(tuple1=>{
      b += (1/(tuple1._2*tuple1._2)).toFloat
    })
    result2.foreach(tuple2=>{
      c += (1/(tuple2._2*tuple2._2)).toFloat
    })
    println("方法3：特征向量归一化的余弦算法文本相似度为：" +"\n"+ (a/(math.sqrt(b)*math.sqrt(c)).toFloat)*100.0+"%")
    /**
     * 方法4：自创的基于调和因子控制的杰卡德方法
     * 杰卡德方法是对两个集合交集元素和并集元素的比值来计算文本相似度的，
     * 忽略了对于词频的计算，比如“南昌你好！”和“南昌你好！南昌你好！南昌你好！”表达的情感是不一样的。
     * 这里引用了词频的调和平均数作为控制因子，使文本相似度对比更加人性化，可以考虑重复语句的表达情感。
     */
    var n = 0.0
    var m = 0.0
    var p = 0.0
    var Length_txt1 = 0.0
    var Length_txt2 = 0.0
    var Length_CommonWords = 0.0
    result1.foreach(tuple3=>{
       n += (1/tuple3._2).toFloat
      Length_txt1 = Length_txt1 + 1.0
    })
    result2.foreach(tuple4=>{
       m += (1/tuple4._2).toFloat
      Length_txt2 = Length_txt2 + 1.0
    })
    result1.foreach(tuple3=> {
      result2.foreach(tuple4 => {
        if (tuple3._1 == tuple4._1) {
          p += (1/tuple3._2 + 1/tuple4._2).toFloat
          Length_CommonWords = Length_CommonWords + 1.0
        }
      })
    })
    val Hn1 = (Length_txt1 + Length_txt2)/(m+n) //计算出两个文本全部词频的调和平均数
    val Hn2 = Length_CommonWords/p //计算出共同词频的调和平均数
    val Control_Factor = Hn2/Hn1 //调和控制因子
    println("方法4：自创的基于调和因子控制的杰卡德方法计算文本相似度为："+"\n" +
      (Length_Intersection/Length_Union)*Control_Factor*100.0.toFloat+"%")
    sc.stop()
  }

}