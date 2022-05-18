# 基于 flink 的电商用户行为数据分析

# 项目介绍

本项目基于对电商用户行为数据，选取三个方向进行分析：

- 热门统计 利用用户的点击浏览行为，进行流量统计、近期热门商品统计等。
- 偏好统计 利用用户的偏好行为，比如收藏、喜欢、评分等，进行用户画像分析，给出个 性化的商品推荐列表。
- 风险控制 利用用户的常规业务行为，比如登录、下单、支付等，分析数据，对异常情况 进行报警提示。


# 软件架构
 本项目分为 6 大模块，全程采用 Flink 流式框架并 使用Scala 编写。

- FlinkTutorial 配合 docs 内容对 Flink 做了全面的代码讲解

- HotItemsAnalysis 热点商品分析

- LoginFailDetect 恶意登录监测

- MarketAnalysis 市场营销分析

- NetworkFlowAnalysis 网络流量分析

- OrderPayDetect  订单支付监测


# 批处理和流处理![](docs/images/lpcl.png)

## 批处理

- 批处理主要操作大容量静态数据集，并在计算过程完成后返回结果。

  可以认为，处理的是用同一个固定时间间隔分组的数据点集合。批处理模式中使用的数据集通常符合下列特征：

  - 有界：批处理数据集代表数据的有限集合
  - 持久：数据通常始终存储在某种类型的持久存储位置中
  - 大量：批处理操作通常是处理极为海量数据集的唯一方法

## 流处理

- 流数据可以对对随时进入系统的数据进行计算。流处理方式无需针对整个数据集执行操作，而是对通过系统传输的每个数据执行操作。流处理中的数据集是 “无边界 ” 的， 这就产生了集合重要的影响：
  - 可以处理几乎无限量的数据，但同一时间只能处理一条数据，不同记录间只维持最少量的状态
  - 处理工作是基于事件的，除非明确停止否则没有 “尽头”
  - 处理结果理可可用，并会随着新数据的抵达继续更新。

# 电商用户行为分析

## 用户与商品

![](docs/images/dsyhxwfx.png)

- 统计分析
  - 点击、浏览
  - 热门商品、近期热门商品、分类热门商品，流量统计
- 偏好统计
  - 收藏、喜欢、评分、打标签
  - 用户画像，推荐列表（结合特征工程和机器学习算法）
- 风险控制
  - 下订单、支付、登录
  - 刷单监控，订单失效，恶意登录（短时间内频繁登录失败）监控

## 项目模块设计

![](docs/images/xmmksj.png)

![](docs/images/xmmksjlr.png)

# 数据源解析

- 用户行为数据

  UserBehaivor.csv       

  e.g.  543462, 1715, 1464116, pv, 1511658000 

- web 服务器日志

  apache.log 

  e.g. 66.249.73.135 - - 17/05/2015:10:05:40 +0000 GET /blog/tags/ipv6

### 数据结构—UserBehavior

| 字段名     | 数据类型 | 说明                                            |
| ---------- | -------- | ----------------------------------------------- |
| userId     | Long     | 加密后的用户 ID                                 |
| itemId     | Long     | 加密后的商品 ID                                 |
| categoryId | Int      | 加密后的商品所属类别 ID                         |
| behavior   | String   | 用户行为类型，包括( ‘pv’, ‘buy’，‘cart’，‘fav') |
| timestamp  | Long     | 行为发生的时间戳，单位秒                        |

### 数据结构—Apache.log 

| 字段名    | 数据类型 | 说明                         |
| --------- | -------- | ---------------------------- |
| ip        | String   | 访问的 IP                    |
| userId    | Long     | 访问的 user ID               |
| eventTime | Long     | 访问时间                     |
| method    | String   | 访问方法 GET/POST/PUT/DELETE |
| url       | String   | 访问的url                    |

# 项目模块划分

- 实时热门商品统计
- 实时流量统计
- 恶意登录监控
- 订单支付失效监控

## 实时热门商品统计

- 基本需求
  - 统计近 1 个小时内的热门商品，每5分钟更新一次
  - 热门度用浏览次数（“pv"）来衡量
- 解决思路
  - 在所有用户行为数据中，过滤出浏览（”PV“）行为进行统计
  - 构建窗口，窗口长度为 1 小时，滑动距离为 5 分钟

![](docs/images/ssrmsptj.png)

- 按照商品 Id 进行分区

  ![](docs/images/ssrmsptj.keyby.png)

- 设置时间窗口

  ![](docs/images/ssrmsptj.time.png)

- 时间窗口（timewindow）区间为左闭右开

- 同一份数据会被分发到不同的窗口

  ![](docs/images/ssrmsptjttim.time.png)

- 窗口聚合

![](docs/images/ssrmsptjagg.png)

- 窗口聚合策略——每出现一条记录就加一

```scala
class CountAgg extends AggregateFunction[UserBehavior, Long, Long]{
	override def createAccumulator(): Long = 0L
	override def add(userBehavior: UserBehavoir, acc: Long): Long = acc + 1;
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}
```

- 累加原则——窗口内碰到一条数据就加一（add 方法）
- 实现 AggregateFunction 接口
  - interface AggregateFunction<IN, ACC, OUT>
- 定义输出结构——ItemViewCount(itemId, windowEnd, count)
- 实现 WindowFunction 接口
  - trait WindowFunction[IN, OUT, KEY, W <: Window]
    - 输入为累加器的类型：Long
    - OUT:窗口累加以后输出的类型为 ItemViewCount(itemId: Long, windowEnd: Long, count: Long), windowEnd 为窗口的结束时间，也是窗口的唯一标识
    - KEY：Tuple 泛型，在这里是 ItemId，窗口根据 itemId 聚合
    - W：聚合的窗口，w: getEnd 就能够拿到窗口的结束时间
    - override def apply

```scala
override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long],
                  collector:Collecctor[ItemCiewCount]) : Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(ItemId, window,getEnv, count))
}
```

- 窗口聚合示例

![](docs/images/ass.png)

- 进行统计整理——keyBy("windowEnd")

![](docs/images/keybY.png)

- 状态编程

![](docs/images/ztbc.png)

- 最终排序输出——keyedProcessFunction
  - 针对有状态的流的底层 API
  - KeyedProcessFunction 会被分区后的每一条子流进行处理
  - 以 windowEnd 作为 key，保证分流以后每一条流的数据都在一个时间窗口内
  - 从 ListState 中读取当前流的状态，存储数据进行排序输出
- 用 ProcessFunction 来定义 KeyedStream 的处理逻辑
- 分流之后，每个 keyedStream 都有其自己的生命周期
  - open：初始化，在这里可以获取当前流的状态
  - processElment: 处理流中每一个元素时调用
  - onTimer：定时调用，注册定时器 Timer 并触发之后的回调操作

![](docs/images/Liststate.png)

定时器触发时，相当于收到了大于 windowEnd + 100 的 watermark，可以认为窗口已经收集到了所有数据，从 ListState 中读取进行处理



## 实时热门页面流量统计

- j基本需求
  - 从 web 服务器的日志中，统计实时的热门访问页面
  - 统计每分钟的 ip 访问量，取出访问量最大的 5 个地址，每 5 秒更新一次
- 解决思路
  - 将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time
  - 构建滑动窗口，窗口长度为 1 分钟，滑动距离为 5 秒

## 实时流量统计—PV 和 UV

- 基本需求
  - 从埋点日志中，统计实时的 PV 和 UV
  - 统计每小时的访问量（PV），并且对用户进行去重（uv）
- 解决思路
  - 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
  - 对于大规模的数据，可以考虑用布隆过滤器进行去重

## 市场营销分析— APP市场推广计划

- 基本需求
  - 从埋点日志中，统计 APP 市场推广的数据指标
  - 按照不同的推广渠道，分别统计数据
- 解决思路
  - 通过过滤日志中的用户行为数据，按照不同的渠道进行统计
  - 可以用 Process function 处理，得到自定义的输出数据信息

## 市场营销分析— 页面广告统计

- 基本需求

  - 从埋点日志中，统计每小时页面广告的点击量， 5 秒刷新一次，并按照不同省份进行划分
  - 对于 ”刷单“ 式的频繁点击行为进行过滤，并将该用户加入黑名单

- 解决思路

  - 根据省份进行分组，创建长度为 1 小时、滑动距离为 5 秒的时间窗口进行统计

  - 可以用 process function 进行黑名单过滤，检测用户对同一广告的点击量，

    如果超过上限则将用户信息以测输出流流出到黑名单中

## 恶意登录监控

- 基本需求
  - 用户在短时间内频繁登录失败，有程序恶意攻击的可能
  - 同一用户（可以是不同 Ip）在 2 秒内连续两次登录失败，需要报警
- 解决思路
  - 将用户的登录失败行为存入 ListState，设定定时器 2 秒后触发，查看 ListState 中有几次失败登录
  - 更加精确的检测，可以使用 CEP 库实现事件流的模式匹配

## 订单支付实时监控

- 基本需求
  - 用户下单之后，应设置订单失效时间，以提高用户支付的意愿，并降低系统风险
  - 用户下单后 15 分钟未支付，则输出监控信息
- 解决思路
  - 利用 CEP 库进行事件流的模式匹配，并设定匹配的时间间隔
  - 也可以利用状态编程，用 process function 实现处理逻辑

## 订单支付实时对账

- 基本需求
  - 用户下单并支付后，应查询到账信息，进行实时对账
  - 如果有不匹配的支付信息或者到账信息。输出提示信息
- 解决思路
  - 从两条流中分别读取订单支付信息和到账信息，合并处理
  - 用 connect 连接合并两条流，用 coProcessFunction 做匹配处理

# 附录 电商常见指标汇总

## 1、电商指标整理

![](docs/images/ren.png)

![](docs/images/huao.png)

![](docs/images/cahng.png)

现在的电子商务：

1、大多买家通过搜索找到所买物品，而非电商网站的内部导航，搜索关键字更为重要；

2、电商商家通过推荐引擎来预测买家可能需要的商品。推荐引擎以历史上具有类似购买记录的买家数据以及用户自身的购买记录为基础，向用户提供推荐信息；

3、电商商家时刻优化网站性能，如 A/B Test 划分来访流量，并区别对待来源不同的访客，进而找到最优的产品、内容和价格；

4、购买流程早在买家访问网站前，即在社交网络、邮件以及在线社区中便已开始，即长漏斗流程（以一条推文、一段视频或一个链接开		始，以购买交易结束）。相关数据指标：关键词和搜索词、推荐接受率、邮件列表/短信链接点入率

## 2、电商 8 类基本指标

1）总体运营指标：从流量、订单、总体销售业绩、整体指标进行把控，起码对运营的电商平台有个大致了解，到底运营的怎么样，是亏是赚。

![image-20200818111654186](docs/images/image-20200818111654186.png)

2）站流量指标：即对访问你网站的访客进行分析，基于这些数据可以对网页进行改进，以及对访客的行为进行分析等等。

![image-20200818111714230](docs/images/image-20200818111714230.png)



3）销售转化指标：分析从下单到支付整个过程的数据，帮助你提升商品转化率。也可以对一些频繁异常的数据展开分析。

![image-20200818111732256](docs/images/image-20200818111732256.png)

4）客户价值指标：这里主要就是分析客户的价值，可以建立 RFM 价值模型，找出那些有价值的客户，精准营销等等。

![image-20200818111749600](docs/images/image-20200818111749600.png)



5）商品类指标：主要分析商品的种类，那些商品卖得好，库存情况，以及可以建立关联模型，分析那些商品同时销售的几率比较高，而

进行捆绑销售，有点像啤酒和尿布的故事。

![image-20200818111810512](docs/images/image-20200818111810512.png)

6）市场营销活动指标，主要监控某次活动给电商网站带来的效果，以及监控广告的投放指标。

![image-20200818111824570](docs/images/image-20200818111824570.png)

7）风控类指标：分析卖家评论，以及投诉情况，发现问题，改正问题

![image-20200818111841801](docs/images/image-20200818111841801.png)

8）市场竞争指标：主要分析市场份额以及网站排名，进一步进行调整

![image-20200818111856947](docs/images/image-20200818111856947.png)



# 参与贡献
1. Fork 本仓库
2. 新建 Feat_xxx 分支
3. 提交代码
4. 新建 Pull Request