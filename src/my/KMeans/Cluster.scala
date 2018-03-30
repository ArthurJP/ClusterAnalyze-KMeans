package my.KMeans

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext


import Array._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by 张俊鹏 on 2017/6/1.
  */
object Cluster {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("K-MeansCluster").setMaster("local")
    val sc = new SparkContext(conf)
    val path="/user/input/all.txt"
    val rawData =  sc.textFile(path)
    println("初始数据：")
    rawData.collect.foreach(println)

    val md = rawData.map( line=> parse(line))
    println("数据清理完毕")
    md.foreach(line=>{
      print("City:"+line.city+"  Data:")
      line.data.foreach(print)
      println
    })

    val labelsAndData=md.map{line=>
      val label=line.city
      val vector=Vectors.dense(line.data)
      (label,vector)
    }
    val data=labelsAndData.values.cache()
    println("从2--8簇进行聚类处理：")
    println("（簇数，标准差）")

    val tempResult=ArrayBuffer[(Int,Double)]()
    (2 to 8).map(k => (k, clusteringScore(data, k))).foreach(i =>{
      tempResult+=i
      println(i)
    } )
    val limit=2000.0
    val k=chooseK(tempResult,limit)
    if(k == -1){
      println("聚类出错！")
      sys.exit(0)
    }
    println("当limit为："+limit+",选择K为："+k)
    val model=new KMeans().setK(k).setMaxIterations(1000).setSeed(6666666666L).run(data)

    val clusterLabelResult=labelsAndData.map{case (label,datum)=>
      val cluster=model.predict(datum)//预测的结果为相应的结果集
      (cluster+1,label)
    }
    println("聚类结果：（类号，省市）")
    clusterLabelResult.sortByKey().foreach{case (cluster,label)=>
      println(f"$cluster%1s$label%20s")
    }
    val rs=clusterLabelResult.map(line=>dataStructure(line))

    val sqlContext=new SQLContext(sc)

//    val schema = StructType(List(StructField("cluster", IntegerType, true),StructField("city", StringType, true)))
    import sqlContext.implicits._

    val cityDF=rs.toDF("cluster", "city")

//    val cityDF = sqlContext.createDataFrame(clusterLabelResult).withColumnRenamed("cluster", "city")

    cityDF.printSchema

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "strongs")
    prop.put("driver","com.mysql.jdbc.Driver")

    cityDF.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/clusterAnalyze", "clustered", prop)

    println("数据已存储到 clusterAnalyze 数据库 ，clustered 表中。")
    sc.stop()
  }

  case class MatchData(city:String,data:Array[Double])

  def toDouble(s: String) = {
    if ("?".equals(s)) 1.00 else s.toDouble
  }

  def parse(line:String)={
    val piece=line.split("\t")
    val city=piece(0)
    val data=piece.slice(1,31).map(toDouble)
    MatchData(city,data)
  }

  def add(md1:MatchData,md2:MatchData)={
    val data=concat(md1.data,md2.data)
    MatchData(md1.city,data)
  }

  def distance(a:Vector,b:Vector)=math.sqrt(a.toArray.zip(b.toArray).map(p=>p._1-p._2).map(d=>d*d).sum)
  /**
    * def zip[B](that: GenIterable[B]): Array[(A, B)]
    * [use case]
    * Returns a array formed from this array and another iterable collection by combining corresponding elements in pairs. If one of the two collections is longer than the other, its remaining elements are ignored.
    * B
    * the type of the second half of the returned pairs
    * that
    * The iterable providing the second half of each result pair
    * returns
    * a new array containing pairs consisting of corresponding elements of this array and that. The length of the returned collection is the minimum of the lengths of this array and that.
    * Implicit
    * This member is added by an implicit conversion from Array[T] to ArrayOps[T] performed by method genericArrayOps in scala.Predef.
    * Definition Classes
    * IndexedSeqOptimized → IterableLike → GenIterableLike
    */
  def distanceToCentroid(datum:Vector,model:KMeansModel)={
    val cluster=model.predict(datum)
    val centroid=model.clusterCenters(cluster)
    distance(centroid,datum)
  }
  def clusteringScore(data:RDD[Vector],k:Int)={
    val seed=6666666666L
    val kmeans=new KMeans().setMaxIterations(1000)
    kmeans.setSeed(seed)
    kmeans.setK(k)
    val model=kmeans.run(data)
    data.map(datum=>distanceToCentroid(datum,model)).mean()
  }

  def chooseK(arrayBuffer: ArrayBuffer[(Int,Double)],limit:Double): Int = {
    val arr = arrayBuffer.toArray
    for (i <- 1 to (arr.length - 1)) {
      if ((arr(i-1)._2) - (arr(i)._2) <= limit) {
        return arr(i-1)._1
      }
    }
    return -1
  }

  def citySubStr(str:String):String={
    var rs:String=null
//    println(str.length)
    if(str.length == 4||str.length == 6){
      rs=str.slice(0,3)
    }else{
      rs=str.slice(0,2)
    }
    rs
  }
  def dataStructure( line:(Int,String) )={
    val cluster=line._1
    val city=citySubStr(line._2)
    (cluster,city)
  }
}
