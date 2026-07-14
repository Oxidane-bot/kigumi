# 设计修订:边上消费投影(consumes)

Status: Draft(待实现验收后并入 DESIGN.md 与契约)
Date: 2026-07-14

## 背景与动机

`files_fn`(item → 消费文件)、`carry_fn`(artifact → 传递上下文)、`aggregate_fn`
(items → 下游聚合视图)是同一个概念的三个实例:**语义投影**——声明一条边上
真正流动的内容。外部需求样本(episode 流水线的 slots 拼装)提供了第四个实证:
消费者从上游产物投影出 prompt 槽位。四个独立形态指向同一抽象,满足实证门槛。

现状的缺口:普通依赖边的键成分取**整个上游产物**的哈希(`upstream:<dep> =
sha(artifact)`)。下游只消费上游的一部分时,上游无关字段的变化仍会失效下游,
缓存精度低于事实;同时框架无法回答"这个节点到底消费了上游的哪部分"
(audit 消费追踪的排队项)。

本修订把消费投影提升为边上的一等声明,一次性解决两件事:投影级缓存精度、
消费关系可审阅。

## 不变式(需求正文;只写不变式,不写实现方式)

1. **声明形态**:`node`/`map`/`scan`/`foreach` 注册与 `Subgraph` 节点声明接受
   可选参数 `consumes: Mapping[str, Callable[[dict], dict]]`,键为已声明依赖名
   (Subgraph 内为本地名),值为纯函数:上游产物 → 本节点消费的视图(dict)。
2. **键语义**:声明了投影的边,键成分 `upstream:<dep>` 的值 =
   `sha(canonical(投影结果))`;未声明投影的边保持 `sha(整个上游产物)`,
   逐字节不变。成分**标签集不变**(仍是 `upstream:<dep>`,不新增标签)。
3. **视图即输入**:节点函数经 `inputs[<dep>]` 拿到的是投影后的视图
   (经 canonical JSON round-trip,与聚合产物同样的字节稳定形态),
   **不是**完整上游产物。投影外的字段对节点函数不可见——这是键精度诚实性的
   结构保证,不是可选行为。
4. **投影源码不入键**:与 `carry_fn` 同一原则——只有本边实际收到的内容才是
   输入事实;项目级源码变更仍由 `libs` 成分兜底。
5. **纯函数契约**:投影必须是其输入的纯函数,返回 JSON 可序列化的 dict;
   违反时报错必须点名节点与依赖名,且 `plan` 与 `run` 对同一投影故障给出
   同形态错误(对齐 carry_fn 的既有裁决)。
6. **注册期校验**:`consumes` 引用未声明依赖 → 注册期 `ValueError`;
   引用 `items_from` / `carry_from` 的源(它们本就不入共享 upstream,消费语义
   由 `item`/`carry` 成分承载)→ 注册期 `ValueError`。
7. **plan / explain / run 三口一致**:同一图同一磁盘状态下,三个入口对投影边
   推导出相同的键成分;上游无关字段变化时 plan 预告 hit、run 实际 hit、
   explain 报告无成分变化;消费字段变化时三口一致报 `upstream:<dep>` 变化。
8. **describe 可审阅**:`describe()` 对每个节点如实报告哪些依赖声明了投影
   (名称级即可,不要求展示函数体);图渲染增强不在本修订范围。
9. **兼容性**:未使用 `consumes` 的注册,行为与键推导路径逐字节不变。
10. **checkpoint 语义不变**:投影不改变审批绑定与"调用过 checkpoint 的执行
    不写 L3"的既有规则。

## 缓存换族裁决

键成分推导新增可选分支,按 cache-key 契约第 7 条与 0.2.0 external 先例,
**`CACHE_SCHEMA` 2 → 3**,有意整体换族,`CHANGELOG.md` 记录。
(备选:不换族,理由是无 consumes 时推导不变;否决——契约条文按字面执行,
先例已立,不为省一次重算引入"推导变了但族没换"的模糊先例。)

## 明确不做(记入否决清单,防止重走)

- 不把 `files_fn`/`carry_fn`/`aggregate_fn` 重构进统一的投影机制——概念统一
  写入文档即可,代码归并无行为收益,徒增换族面。
- 不提供 ModelNode/PromptNode 式第二注册 API:节点函数体剩下的 slots 拼装 +
  `ctx.repair` 就是业务事实本身;consumes 落地后其可见性诉求自动满足。
- 不做投影的逆向静态推导(从函数体 AST 猜消费字段):猜测违反诚实原则。

## RED 用例清单(先红后绿;命名可调,断言意图不可减)

1. 上游产物**未被消费**的字段变化 → 下游 run 命中缓存、plan 预告 hit。
2. 上游产物**被消费**的字段变化 → 下游失效,explain 点名 `upstream:<dep>`。
3. 节点函数拿到的 `inputs[<dep>]` 只含投影视图,访问投影外字段失败。
4. 投影函数源码变化但输出内容不变 → 键不变
   (对照 `test_scan_carry_fn_code_is_irrelevant_when_extracted_content_is_equal`;
   投影函数定义在 source_dirs 之外)。
5. `consumes` 引用未声明依赖 / items_from 源 / carry_from 源 → 注册期 ValueError。
6. 投影返回不可 JSON 序列化或非 dict → 报错点名节点与依赖名。
7. 投影函数抛异常 → plan 与 run 报同形态错误(节点 + 依赖上下文)。
8. 投影输出的 dict 键序不影响键与下游输入字节(canonical round-trip)。
9. 键成分标签集锁定测试保持通过(不新增标签)。
10. `describe()` 报告投影声明;未声明的节点无此噪音。
11. Subgraph 节点声明 consumes,mount 后按本地名生效且键成分用本地标签。
12. map/scan 的共享依赖(非 items/carry 源)上声明投影,对每个 item 生效。
13. `CACHE_SCHEMA=3` 进入 `kigumi` 成分(对照既有 schema 锁定测试更新)。

## 随行文档变更(同一提交)

- `docs/contracts/cache-key.md`:不变式 2/3 补投影语义、schema 记录升 3、
  Verification 增补新锁定测试。
- `DESIGN.md`:L3 缓存键说明补一句消费投影;修订记录加一条;
  "已否决"处记 ModelNode 与三 fn 归并。
- `CHANGELOG.md` `[Unreleased]`(中文):新增 consumes、有意换族声明。
- `docs/adoption.md`:consumes 用法一小节(何时用:下游只消费上游一部分时)。

## 验收标准

- 全部 RED 用例转绿;`uv run pytest -q`、`uv run ruff check .`、
  `uv run ruff format --check .` 全过。
- cache-key 契约的既有锁定测试除 schema 常量外不因本修订改语义。
- 既有示例不需要任何改动即可通过现有测试。
