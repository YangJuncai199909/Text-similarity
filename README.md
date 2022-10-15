# Text-similarity
【摘要】 本次大数据基础的课程期末大作业主要是运用spark对平台分析文本相似度，以及对数据进行动态显示。对于数据的处理和计算有一定的难度，需要全面理解整个流程情况，才能写出自己的代码。文本相似度方面尝试了余弦距离、simHash、杰卡德方法、汉明距离等方法，最后基于余弦距离的方法，总结出来调和平均向量角度算法。温度采用了计算平均温度的方法体现温度的变换趋势。

【关键词】spark平台；大数据基础；文本相似度；变化趋势

1文本相似度
1.1项目简述
运用spark平台分析比较两篇英文文献相似度。
（1）第一篇：A Tale of Two Cities - Charles Dickens
（2）第二篇：David Copperfield - Charles Dickens
两篇文档都是.txt文件形式。要求：采用Scala语言编写Spark程序运行，可以采用spark的本地运行模式（local）。文献相似度可以使用任意的相似度定义（常用的相似度定义见https://zhuanlan.zhihu.com/p/88938220）。提交项目源代码及项目报告（需要包含选用的相似度说明，以及运行结果的截屏。）（60.0分）
1.2 文本分词
在之前的学习过程中，我们学到了词频的统计(WordCount)，这对我们理解文本的词频处理给予了很大的帮助。我们采用本地(local[*])多线程模式运行spark平台，然后将两篇文本导入成RDD[String]格式，再对文本通过flatmap进行分词和替换。对于文本中出现的“，”、“.”、“’”、“:”、“;”、“””、“—”、“?”、“！”、“  ”（两个空格）全部替换成“ ”（一个空格），然后再对文本进行词频的划分，根据空格为分隔符进行文本的划分处理。代码如下：
object YjcBigDataFinal_Similarity {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("Similarity")
    val sc = new SparkContext(conf)
    val lines1: RDD[String] = sc.textFile("./A Tale of Two Cities - Charles Dickens.txt")
    val lines2: RDD[String] = sc.textFile("./David Copperfield - Charles Dickens.txt")
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
对两个文本都是进行相同的处理，也可以用replaceAll( )，采用正则表达式的方式处理文本非单词内容。
1.3 词频统计
有一些方法需要对词频进行统计，从而得到特征向量的表达式，如余弦距离法；有一些则无需对词频进行统计，如杰卡德方法。这里先将词频进行一个统计，用以后面算法的需要。
先将分词好的RDD依次映射到元组中，RDD格式变为RDD[(String,Int)]，这时候每一个单词都有自己的一个元组。代码如下：
val pairWords1:RDD[(String,Int)] = words1.map(word=>{
  new Tuple2(word,1)
})
val pairWords2:RDD[(String,Int)] = words2.map(word=>{
  new Tuple2(word,1)
})
将元组中的数据进行reduceByKey操作，统计词频并写入元组中。代码如下：
/**
 * reduceByKey 先分组，后对每一组内的Key对应的value去聚合,
 * reduceByKey的作用就是对相同key的数据进行处理，最终每个key只保留一条记录
 */
val result1 = pairWords1.reduceByKey((v1:Int,v2:Int)=>{v1+v2}).collect()
val result2 = pairWords2.reduceByKey((v1:Int,v2:Int)=>{v1+v2}).collect()
val result3 = pairWords1.reduceByKey((v1:Int,v2:Int)=>{v1+v2})
val result4 = pairWords2.reduceByKey((v1:Int,v2:Int)=>{v1+v2})
注：reduceByKey的作用对像是(key, value)形式的rdd，而reduce有减少、压缩之意，reduceByKey的作用就是对相同key的数据进行处理，最终每个key只保留一条记录。保留一条记录通常有两种结果。一种是只保留我们希望的信息，比如每个key出现的次数。第二种是把value聚合在一起形成列表，这样后续可以对value做进一步的操作，比如排序。
1.4 方法1：杰卡德相似度方法
1.4.1 杰卡德方法介绍
杰卡德相似度一般被用来度量两个集合之间的差异大小。假设我们有两个集合A和B，那么二者的杰卡德相似度为：

杰卡德距离的思想非常简单:两个集合共有的元素（交集）越多，二者越相似；为了控制距离的取值范围，我们可以增加一个分母，也就是两个集合拥有的所有元素（并集）。
1.4.2 代码算法思想
先将之前统计好词频的RDD进行集合的交并运算，然后设置两个可变变量（一定要为浮点型，否则后面的除法运算会显示结果为0）来进行无数据流的循环操作来分别计算交集和并集的长度。然后用交集长度比上并集长度算出文本的相似度情况。代码如下：
/**
 * 杰卡德相似度方法
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
println("杰卡德方法计算文本相似度为："+(Length_Intersection/Length_Union)*100.0.toFloat+"%")
1.4.3 运行结果分析
最后计算得到的结果约为4.95%，运行结果如图所示：

在汉语相似度比较中，单一的比较交并集忽略了文本的长度差异，但是在英语文章的比较中，单词之间不需要考虑连续重复的问题。但是如果要考虑包含语气加重的重复，则需要在分母添加一个惩罚因子，来对保证区别出重复语句和单一语句在语境上表达意思的不同。这里因为文本过大，没有具体的实际案例去进行惩罚因子的估算，所以只采用了单一的杰卡德算法。
1.5方法2：基于余弦距离的文本相似度方法
1.5.1 余弦距离的定义
余弦距离来源于向量之间夹角的余弦值。假设空间中有两个向量：


那么二者夹角的余弦值为：

向量的夹角余弦值可以体现两个向量在方向上的差异，因此我们可以用它来度量某些事物的差异或者说距离。
1.5.2 余弦距离在文本相似度中的应用
在文本相似度计算任务中，为了让数据适合余弦夹角的概念，人们假想：语义空间中存在一个“原点”；以原点为起点、文本特征数组(这里为了避免与“语义向量”混淆，借用了“数组”这个名称)表示的点为终点，构成了一个向量，这就是代表了文本语义的“语义向量”。语义向量的方向，描述了文本的语义。因此，我们可以用两篇文档的语义向量的夹角余弦值来表示它们的差异。这个余弦值通常被称为“余弦距离”。余弦值越接近1，就表明夹角越接近0度，也就是两个向量越相似，这就叫"余弦相似性"。
需要注意的是，余弦距离的运用需要获得对比文本的特征向量，再用特征向量去进行叉乘和点乘运算。
相似度的计算公式为：

1.5.3 代码算法思想
在查找学习过相关资料后，针对文本自身，我想到了一种简易的求解特征向量值的方法。这个方法不要去专门求特征向量，因为特征向量必须是以共有词为参照的，所以可以直接从公式出发，分步求解分子和分母。

（1）分子部分：
两个文本的特征向量可以分成重复单词部分和不重复单词部分。
文本特征向量示意图（假设）
重复部分特征向量按照词频进行对比	非重复部分特征向量对比
文本1	(A单词)
Freq:1	(B单词)
Freq:2	(C单词)
Freq:3	(D单词)
Freq:1	(F单词)
Freq:0	(G单词)
Freq:2	(H单词)
Freq:3
文本2	(A单词)
Freq:3	(B单词)
Freq:4	(C单词)
Freq:6	(D单词)
Freq:0	(F单词)
Freq:3	(G单词)
Freq:0	(H单词)
Freq:0
从表格中的假设数据可以得出该文本的特征向量为：
文本1 = (1,2,3,1,0,2,3)
文本2 = (3,4,6,0,3,0,0)
由此可以看出，分子部分两个的乘积只对重复部分有值，对于非重复部分，值恒为0*N=0的形式，所以只需要计算出重合部分的词频互乘值就可以。
代码实现如下：
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
对两个文本共有的单词的词频进行平方求和，用可变变量x储存。
x表示：
（2）分母部分：
将每个文本的词频通过循环的方法进行遍历，再对遍历得的词频值进行平方求和，再开二次方根运算。
代码实现如下：
result1.foreach(tuple1=>{
  y += tuple1._2*tuple1._2
})
result2.foreach(tuple2=>{
  z += tuple2._2*tuple2._2
})
把第一个和第二个文本分别进行词频的平方求和，用可变变量y和z储存。
y表示：；z表示：
然后把x,y,z代入公式进行计算，可得：

代码实现如下：
val cos = (x/(math.sqrt(y)*math.sqrt(z))).toFloat //求出余弦值
val similarity = (1.0-cos)*100.0
println("方法2：余弦距离算法文本相似度为：" +"\n"+ similarity + "%")
注意：因为cos的值是一个小数，所以一定要记得在后面加.toFloat，否则输出结果显示为0。
得到余弦值之后，便可通过余弦相似度计算公式：

计算得到相似度。
1.5.4 运行结果分析
最后进行代码运行，得到相似度结果约为：11.97%

但是余弦算法在对一些大文本进行分析时效率太低，需要循环遍历整个文本对词频进行累加，计算量太大。所以余弦方法适用于文本较短，也就是特征维度较低的场景。
1.6 方法3：（自创）特征向量归一化的余弦算法
1.6.1 算法思想介绍
假设文本足够大且词频足够大的情况，可以把两个文本的每个词频的倒数看成是归一化的M维向量分量值。即

注：M为词频，N为向量维度
考虑到文本数据量过大，惯用的余弦方法分母亿级开根号可能会造成很大的误差，所以采用归一化的的思想，取倒数求和，假设趋于无穷时和为一，对于亿级数据的根号误差有着良好的缓和作用。但是它的弊端显而易见，极易受极端值影响，比如出现词频为1的单词就会对其影响很大。所以这种方法适用于文本很大且词频很大的文本，比如音标教材等（一本书中一个音标会重复出现很多次）。它的公式如下（自创）：

1.6.2 运行结果分析
代码如下，将上述公式用代码实现，效仿余弦距离的公式。
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
运行结果如下：

运行结果文本相似度约为15.77%，这是一个较高的数值，可见此种方法对于这两个文本的适用性并不高。一个是文本不够庞大，还有一个致命的缺陷是前提假设的词频无穷大导致各向向量归一化的条件在这两个文本中无法成立。随便一个1词频的单词就会使得归一化失败。这种方法只是一个尝试，作为初步追求创新的失败品呈现了出来。
1.7 方法4：（自创）基于调和因子控制的杰卡德方法
1.7.1 算法思想介绍
杰卡德方法是对两个集合交集元素和并集元素的比值来计算文本相似度的，忽略了对于词频的计算，比如“南昌你好！”和“南昌你好！南昌你好！南昌你好！”表达的情感是不一样的。
这里引用了词频的调和平均数作为控制因子，使文本相似度对比更加人性化，可以考虑重复语句的表达情感。
调和平均数的作用是用来计算一个当一个样本不知道总体单位数只知道每组的单位数的情况下，求解平均数的一种方法。这里通过分词过后只知道每个单词的词频，也就是每组的单位数，总体的词频我们并不知道。通过求调和平均求出交集的词频平均数和总集的词频平均数，一是极大程度减少运算量，避免总词频的计算；二是制造出一个受分组词频影响的词频因子，使之可以控制杰卡德系数因为集合元素之比造成的词频误差。
公式如下：

1.7.2 运行结果分析
代码如下：
（1）设置可变变量方便后续值的计算
var n = 0.0
var m = 0.0
var p = 0.0
var Length_txt1 = 0.0
var Length_txt2 = 0.0
var Length_CommonWords = 0.0
（2）计算交并集长度以及词频的调和平均数的分子
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
（3）计算出交并集的词频调和平均，仿造Jaccard系数得到调和控制因子，在代入到式子中，与Jaccard系数相乘，得到受频率调和因子控制的Jaccard相似度。
val Hn1 = (Length_txt1 + Length_txt2)/(m+n) //计算出两个文本全部词频的调和平均数
val Hn2 = Length_CommonWords/p //计算出共同词频的调和平均数
val Control_Factor = Hn2/Hn1 //调和控制因子
println("方法4：自创的基于调和因子控制的杰卡德方法计算文本相似度为："+"\n" +
(Length_Intersection/Length_Union)*Control_Factor*100.0.toFloat+"%")
（4）运行结果为如下：

可以看出文本相似度约为4.06%，较原始的杰卡德算法有了明显的准确度提高。在元素种类和词频方面同时考虑，弥补了原始杰卡德算法的不足，提升了文本对比精准度。

2 前苏联气候变化趋势分析
2.1项目简述
运用spark平台分析前苏联气候变化趋势特征。
数据来源：https://cdiac.ess-dive.lbl.gov/ftp/ndp040/
数据说明：https://cdiac.ess-dive.lbl.gov/ftp/ndp040/data_format.html
要求：采用Scala语言编写Spark程序运行，可以采用spark的本地运行模式（local）。
（1）按照时间窗口统计单个观测点气温的变化趋势；时间窗口可以以小时、日、周、月为单位，观测序列可以是连续时间窗口，也可以是周期性地的时间窗口
（2）同一个时间窗口内各个观测点气温的变化趋势。
（3）上述两问中的气温可以最低温度、最高温度、平均温度
2.2单个观测点的气温变化
选取观测点f29263,它内部记录了从1884年1月1日至2001年12月31日的观测数据。
点开dat文件，发现每行有15个数据位，也就是0—14位表示不同的含义。经过查表得到，6、8、10数据位对应当日的最低气温、平均气温和最高气温
2.2.1 解决日志数据显示问题
因为数据量显示太大，我从网上找到了两种方法将spark日志级别设置为ERROR，即不显示正常的日志，只显示错误日志。
（1）方法1：代码里设置
val sc = new SparkContext(conf)
sc.setLogLevel("ERROR")
这样它就只会显示错误的日志。
（2）方法2：log4j.properties文件设置
新建一个log4j.properties。将级别改为ERROR
# Set everything to be logged to the console
log4j.rootCategory=ERROR, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=ERROR

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark_project.jetty=ERROR
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=ERROR
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=ERROR
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
2.2.2 原始数据处理
导入一个站点的文件（切忌不要全部导入），对数据进行空格符的替换和分割，然后针对标志位和数据位进行过滤。


由数据文档可知，第11位不能为9，第10位不能为999。显示9表示数据未被观测。代码如下：
val fields = pre_data1.map(_.trim.replace("   ", " ")
  .replace("  ", " ").split(" "))
//保留15个数据域的数据，并且符合QR为有观测数据和温度值不为999.9。
//11位不能为9，,6\8\10位标志位不能为999，对数据用filter进行过滤
val pre_ann = fields.filter(_.length == 15)
  .filter(_(9)!="999").filter(_(10)!="9")
过滤完之后的数据开始进行RDD的映射操作，取每月的最高气温的平均值来表示气温的变化情况。先选择数据位，0表示站点ID，1表示年份信息，2表示月份信息，9表示日最高气温。
使用groupByKey将映射完的RDD进行分组压缩合并，在通过sortByKey将数据按照年份和月份进行从小到大的排序。
/**
 * 第一问：按照时间窗口统计单个观测点气温的变化趋势（以每日最高温度为依据），以月为单位
 */
val pre_ann_map = pre_ann.map(f=>((f(0),(f(1),f(2).toInt)),f(9).toFloat))
//对过滤数据进行分组压缩，再进行排序处理。
val High_Temp: RDD[((String, (String, Int)), Iterable[Float])] = pre_ann_map.groupByKey().sortByKey()
//rdd1.foreach(x=>{println(x)})
//求出每月最高气温的平均值
val High_Temp_mean: RDD[((String, (String, Int)), Float)] = High_Temp.map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
//pre_ann_mean1.foreach(x=>{println(x._1)})
High_Temp_mean.foreach(x=>{println(x)})
将排序后的数据进行reduce操作，不会根据key来进行合并，而是单独合并，这样才能显示每年每月的最高气温平均值。将每月的数值相加后除以每月天数，得到每月最高气温的平均值。在通过循环输出内容。
2.2.3 运行结果和分析
运行结果如下图：


观测点在1887年5月份才开始进行观测最高气温的观测。有某些月份因为战争或其他原因观测数据为999，被过滤抛弃。在2001年12月31日进行最后一次气温观测，所以最后的数据为2001年12月份数据。
综合所有数据可以看出，每年的固定月份，其气温都大致相同，总体温度变化不大。对于不同的月份，5月份的温度明显升高，前苏联该观测点进入夏令时；在10月份，温度明显下降，前苏联该观测点进入冬令时，且气温都为零下。可以看出，在1887年到2001年期间，该观测点的测量数据较为稳定，月份的气温变化较为符合俄罗斯的常规变化。
2.3 各个观测点气温的变化趋势
2.3.1 过滤设置
各个观测点气温的变化趋势主要通过各个观测点固定一个月份的温度的均值和方差来体现。在第一问的基础上，对后面的数据处理进行一些更改。
设置显示数据为每个观测点每年第八月的数据。
val pre_ann1 = fields.filter(_.length == 15).filter(_(2)=="8")
  .filter(_(5)!="999").filter(_(6)!="9")
求出每个站点每年八月的平均温度，在输出每个站点每年8月的平均气温。
val pre_ann_map1 = pre_ann1.map(f=>(((f(1),f(2).toInt),f(0)),f(5).toDouble))
val Mean_Temp: RDD[(((String, Int), String), Iterable[Double])] = pre_ann_map1.groupByKey().sortByKey()
val pre_ann_mean2: RDD[(((String, Int), String), Double)] = Mean_Temp
  .map(x=>(x._1,x._2.reduce(_+_) / x._2.size))
2.3.2运行结果分析
运行结果如图所示：


通过每个站点的每年8月的平均气温来体现温度的变化情况。可以发现，由于维度地势等原因，各观测点的八月份的气温有所不同，大致分布在13摄氏度左右。
