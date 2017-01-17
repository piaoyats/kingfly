#QAs for refer

## 什么时候会触发TaskSchedulerImpl的resourceOffers？
  有任务提交，有executor注册，有资源更新，有任务失败，（但从打印信息来看每隔指定时间间隔就会，有一个定时机制在这里？）

## 如何debug spark
```  
bin/spark-submit 

  --class org.apache.spark.examples.SparkPi 

  --master local 

  --driver-java-options '-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=8765' 

  examples/target/scala-2.10/spark-examples-1.1.0-SNAPSHOT-hadoop1.0.4.jar 50 

```



## 在for循环中对RDD进行操作，那么RDD的操作中会用到的参数需要在for循环里面声明。
如：
  ```
  val zone: Zone = new Zone(…);
  val arrive: Boolean = …
  for (…)
  {
       …
       val timesInOneDay: Int = rddDataInTimeSegment.map(line => imsiMapper(line))
                  .groupByKey()
                  .map(records => getTimesOfOneUser(records, zone, arrive))
                  .reduce(_ + _);
         …
   };
  ```
  这样使用会在红色的代码处抛出异常：
  java.lang.IllegalArgumentException: argument type mismatch
  注意：单机情况下不会出现这种情况，集群模式才会，原因待分析。
  将代码改为：
```
  for (…)
  {
       …
  val zone: Zone = new Zone(…);
  val arrive: Boolean = …

       val timesInOneDay: Int = rddDataInTimeSegment.map(line => imsiMapper(line))
                  .groupByKey()
                  .map(records => getTimesOfOneUser(records, zone, arrive))
                  .reduce(_ + _);
         …
   };
```
  程序在单机和集群模式下均能正常运行。
```
  中间还尝试过将zone和arrive这两个变量设为全局变量，红色代码改为
  .map(records => getTimesOfOneUser(records))
  这样在单机模式下也没有问题，但是在集群模式下在getTimesOfOneUser函数中使用zone的代码处会抛出空指针异常。
  zone的声明如下：
  private var zone: Zone = _;
  在main函数中使用zone之前会对其赋值，但是赋值不起作用。原因待分析。
```
## spark-local-20130923113506-9bc3/15/shuffle_0_123_98这些文件是溢出文件还是其他什么东东，这些数字又是什么含义？
  数据在内存中无法存放（MemoryStore）时，会写入本地磁盘（DiskStore）。Shuffle数据必定会写入磁盘。
  数字含义：第一个数字是该shuffle RDD的ID，第二个是父RDD的partition序号，第三个
  是reduceID，对应于该shuffle RDD的partition的序号。

## shuffled rdd fetch过程是怎样的
```
通过shufflemanager 获取reader，（shuffle handler）
具体fetch时，通过BlockStoreShuffleFetcher去fetch
而BlockStoreShuffleFetcher上实际是通过ShuffleBlockFetcherIterator来获取数据的，这个获取回来进行一个解包得到最后的iter.
    BlockStoreShuffleFetcher 根据shufflid，reduceid（shuffled rdd要计算的partition的index）来确定去哪些节点获取哪些block
    数据.具体是怎么确定去哪个节点拿哪些block是通过mapOutputTracker来实现的。 
    SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
ShuffleBlockFetcherIterator是一个(BlockID, values)的迭代器，获取数据需要如下信息：
  1） blockmanager --- 本机的blockmanager, 用来读本地的block
  2） blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] ---- 去哪些节点拿哪些blocks
  3） blockTransferService 或者 shuffleClient --- fetch 远端数据
  4） 序列化器 --- 反序列化数据
  5） 一个重要的配置项spark.reducer.maxMbInFlight --- 同时fetch的最大数据大小,和每次fetch request的大小，最多允许同时5个去远端fetch
```
```
  官方的解释
  spark.reducer.maxMbInFlight	48	
  Maximum size (in megabytes) of map outputs to fetch simultaneously from each reduce task. 
  Since each output requires us to create a buffer to receive it, this represents a fixed 
  memory overhead per reduce task, so keep it small unless you have a large amount of memory.
```
```
  在ShuffleBlockFetcherIterator中
  1） hasNext很好确定只要已经处理的blocks数目没有达到需要fetch的block数目就是true
  2） 内部维护一个fetch result队列，这个队列在 发送远程/本地 fetch请求且等待fetch成功后 将数据流添加到 到这个队列
  3） 内部维护一个bytesInFlight变量，每次发送一个fetch请求时，就将本次请求要fetch的数据大小添加到该值，等着吃fetch的
  数据得到并被消费后再减掉，也就是说最后这个值一定为0，否则fetch失败
  4） next中有一个根据maxMbInFlight来判断是否发送fetch请求的逻辑：只有在fetchRequests不为空且即将fetch的数据加上
  bytesInFlight不超过maxMbInFlight才会发送fetch请求
```
## fetchrequest 是如何切的？
  根据spark.reducer.maxMbInFlight的配置值，比如配置50m，则除以5得到10M，则按10M来切fetch request。
  如有10个block，1：2，2：5，3：4...这将123的请求包装为一个request,这里的block是和什么对应？

## executor 启动是怎么获得driver端的配置的？
  通过akka来实现的，起一个专门获得配置的actor，获得后停掉，比较巧妙：
```
  // Bootstrap to fetch the driver's Spark properties.
  val executorConf = new SparkConf
  val port = executorConf.getInt("spark.executor.port", 0)
  val (fetcher, _) = AkkaUtils.createActorSystem(
   "driverPropsFetcher", hostname, port, executorConf, new SecurityManager(executorConf))
  val driver = fetcher.actorSelection(driverUrl)
  val timeout = AkkaUtils.askTimeout(executorConf)
  val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
  val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
          Seq[(String, String)](("spark.app.id", appId))
  fetcher.shutdown()
```
## blockId 和真实的block数据是怎么对应上的？为什么shuffle fetch的时候是通过 
   ShuffleBlockId(shuffleId, s._1, reduceId) 组装出来的？
```
   首先通过MapOutputTracker获取要到每个节点fetch的output size,待续
   /**
      * Called from executors to get the server URIs and output sizes of the map outputs of
      * a given shuffle.
      */
     def getServerStatuses(shuffleId: Int, reduceId: Int): Array[(BlockManagerId, Long)] = {
```
## MapOutputTracker 原理解析？
```
   顾名思义，主要用于追踪map task的output location，由于driver 和 executor（PR） 使用不同的map存储元数据，
   所以对应driver和executor，分别有两个实现。这里有一个很重要的概念：MapStatus(也是一个接口)，是
   shufflemaptask的输出，用于描述这个任务在哪个blockmanager 上执行，且可以获得该task吐出给指定reduce 的数据大小，
   视为一个block。MapOutputTracker 在哪初始化的？ SparkENV里面初始化的MapOutputTracker 在哪里用的？ master 侧的
   在DAGScheduler用的，是在任务执行完成时调用,handleTaskCompletion,注意这里是在一个stage的tasks执行完了后一下加到
   tracker的。当中task执行完成是加到stage 的 outputLocs(= Array.fill[List[MapStatus]](numPartitions)(Nil)); 
   executor 端: executor的shufflemaptask 的返回是直接通过statusUpdate发送到driver端的.   shuffleRDD 获取每个
   redueceId的数据时，在BlockStoreShuffleFetcher通过tracker获取别的executor的reduce数据：
   val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId).
   其实getServerStatuses方法，首先在自己节点的mapStatuses查找，没有的话到driver端去拿，通过actor，
   也就是说mapStatuses是汇总维护在master处的，这里是否可以改？
```
## fetch 失败后的逻辑？
```
   DAG中会标识这个stage 已失败，且标记这个executor失败并移除.这里的todo，允许一个executor fetch 失败多次。
   这里有两个疑问：
   12.1 handleExecutorLost of DAGScheduler 中的
         if (!env.blockManager.externalShuffleServiceEnabled || fetchFailed) {
           // TODO: This will be really slow if we keep accumulating shuffle map stages
           for ((shuffleId, stage) <- shuffleToMapStage) {
             stage.removeOutputsOnExecutor(execId)
             val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray  // 为什么是head？
             mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
           }
   12.2  removeOutputsOnExecutor of Stage
      for (partition <- 0 until numPartitions) {
         val prevList = outputLocs(partition)
         val newList = prevList.filterNot(_.location.executorId == execId)
         outputLocs(partition) = newList
         if (prevList != Nil && newList == Nil) { // 这个判断用来干嘛
           becameUnavailable = true
           numAvailableOutputs -= 1
         }
       }
```
## 每个executor tracker 拿到 MapStatus 后，是如何转换为一个 Array[(BlockManagerId, Long)] ?
```status =&gt; (status.location, status.getSizeForBlock(reduceId)), 所以这里也进一步明确block的概念
   就是一个task针对一个reduce的输出， 即 mapid and reduceid --- 对应一个block
```
## 如何调试序列化问题
   添加 -Dsun.io.serialization.extendedDebugInfo=true

## 如何使得rdd saveastextfile时覆盖已有的目录
   设置 spark.hadoop.validateOutputSpecs = false






