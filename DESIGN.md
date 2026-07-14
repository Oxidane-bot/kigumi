# kigumi 设计文档

木组(kigumi):无钉咬合的木工工艺。本库是 LLM 内容流水线的承重结构层——
项目(屋顶)与模型(立柱)之间靠精确咬合连接,不合榫就打回重做。

## 定位

给"用 coding agent 开发 LLM 流水线"的场景提供地基:上下文注入、结构化校验与
修复、确定性重放、DAG 编排,以及一套让规矩自动执行的工具链。

主要用户是 coding agent:agent 的行为由环境反馈塑形,库必须把"正道"做成
阻力最小的路径,把"歪路"做成走不通的路径。

## 起源与实证依据

抽取自三个内部实战项目的共同模式,干净重写,不搬运代码。旧代码注释里的每条 live 教训
(缓存不动点、回显截断不收敛、extra_forbidden 剥壳、键序漂移换缓存族等)
转化为本库的 RED 测试用例,而非复制实现。

已被否决的路线,不再重走:
- 重基建编排(Temporal/server/DB):内部实验已证死路。
- 纯 vendored 模板:漂移成本实证过高(内部项目的三个修复环变体)。
- 采用现成 DAG 库:2026-07 调研结论,无一同时提供内容寻址缓存(含代码
  哈希)+ 人工检查点 + 确定性重放;且 helper 哈希与 prompt 文件哈希
  是自研 runtime 独有的领先点。
- 另造 ModelNode/PromptNode 注册 API:普通节点配合边上的 `consumes` 已能显式约束消费视图，
  第二套节点体系只会制造重复语义。
- 把 `files_fn`/`carry_fn`/`aggregate_fn` 归并为统一投影实现:概念同属语义投影即可，代码归并
  没有行为收益，反而扩大缓存换族和回归面。

## 包结构

```
kigumi/
├── _declarations.py #     声明期缓存策略、外部指纹与单段名称校验
├── _execution.py   #     私有执行信封:缓存、审批、物化与 sidecar
├── __init__.py     #     顶层公开 API
├── artifacts.py    #     原子写入、规范 JSON 与 sidecar 基础件
├── blobs.py        #     内容寻址二进制仓
├── calling.py      # L1  调用:内容寻址缓存 / dry-run 熔断 / 溯源 / 预算记账
├── cli.py          #     项目运维 CLI:kigumi
├── config.py       #     项目配置、路径发现与环境装载
├── dag.py          # L3  编排:@node / foreach / checkpoint / 节点缓存
├── enforce.py      #     守卫检查器
├── errors.py       #     跨调度/存储边界的公开异常
├── evals.py        #     评估与进化层的指标、评委与门禁
├── optimize.py     #     评估与进化层的提示词候选演化
├── prompt.py       # L1.5 拼接:inject / 严格渲染 / Section / schema 格式段 / clip
├── repair.py       # L2  修复环:repair_loop + call_validated;rebuild/continue 双模式
├── slots.py        #     跨进程限流与自适应容量
├── store.py        #     存储布局:run、缓存、归档、物化、审批与 GC
├── subgraph.py     #     静态可复用子图声明;挂载与执行仍归 Dag
├── testing.py      #     pytest 插件(面向用户项目)
└── transport.py    # L0  传输:complete(messages);LiteLLM/stdlib 可选适配器
```

依赖:pydantic 是唯一内核依赖;litellm 为 optional extra(`kigumi[litellm]`,
transport 已惰性导入)。LiteLLM 只是可选适配器；任意 SDK 都可实现 `Transport`，
stdlib 适配器保证无 extra 也可用。L0–L2 不依赖 L3;
命令式链项目只取底层。pytest11 入口在非 kigumi 项目中必须严格 no-op。

## 项目配置约定(`[tool.kigumi]`)

库装进 site-packages 后的发现问题统一由项目 pyproject 的 `[tool.kigumi]` 回答,
全部有默认值,零配置可跑:

```toml
[tool.kigumi]
prompts_dir = "prompts"          # 严格渲染与 dry-render 测试的模板根
artifacts_dir = "artifacts"      # runs/ 与缓存的根(cache 在其下 _cache/)
source_dirs = ["nodes", "lib"]   # helper 哈希与 guard 扫描的目录集(=同一集合)
env_file = ".env"                # KIGUMI_MODEL_DEFAULT / KIGUMI_MODEL_PRO / api keys
```

`kigumi runs list` / `diff` / `gc` / guard / dry-render 全部从这份配置解析路径。
各层不得自带另一套发现逻辑。

## 各层要点

### L0 transport
- 唯一接口 `complete(messages, model, **params) -> Response`
  (text / usage / finish_reason / reasoning / model),外加
  `resolve(model) -> str`(别名 → 具体模型名,供 L1 构缓存键)。
- LiteLLM 与纯标准库 OpenAI 兼容实现都是可选适配器；任意 SDK 均可实现
  `Transport` 协议。自愈策略共享,不复制。
- **params 契约(归一化词汇表,冻结于 P1.1)**:`json_mode=True` →
  `response_format={"type":"json_object"}`;`system="…"` → 前插 system 消息;
  `reasoning_effort` 透传;其余透传。词汇表进缓存键语义,事后扩充只增不改。
- 故障自愈(全部有界):429/5xx 指数退避、`finish_reason=length` 且调用方
  显式设了 max_tokens → 加倍重试(至多 2 次);未设 max_tokens 的截断 →
  抛 TruncatedResponseError(静默返回残文即下游投毒);空响应 → 退避重试,
  耗尽即抛 EmptyResponseError,**绝不返回空文本**。
- StdlibTransport 必须带默认超时(300s);所有重试路径都有退避 sleep。
- 多档模型别名(default/pro)映射到不同模型/供应商,节点只说档位。

### L1 calling
- `LLMCaller(transport, cache_dir, seed, budget, dry)`,依赖注入,测试可替换。
- 内容寻址缓存:键 = messages + **resolved model**(经 transport.resolve,
  换 .env 模型 = 换缓存键,防跨模型回放旧答案)+ params + seed;
  撕裂缓存按 miss;**空响应拒入缓存**(transport 违约时的第二道闸)。
- 溯源 meta 同记 model_alias 与 resolved model;**先记 calls 再 budget.record**
  (超限异常不得吞掉最贵那次调用的溯源)。
- dry-run 熔断:到达真调用即抛错;缓存命中时不熔断继续走(特性,非 bug:
  dry-run 的定义是"不发新请求",不是"不消费历史")。
- 线程安全:Budget 加锁;calls 加锁;同键 in-flight 去重(并发同键只打一次)。
- seed 仅是缓存命名空间,不传给 provider,不承诺采样可复现。
- 大块内容按引用:消息体里的大负载(视频/图像 base64)以
  `{"kigumi_file": 路径}` 内容件表示,缓存键取文件内容哈希,缓存 payload
  存引用不存字节(接口 P1.1 预留,实现随 P6a)。
- 跨进程限流(fcntl.flock 请求槽,多进程共享 provider 配额)归 P6a。
- token 预算记账,超限中止 run。

### L1.5 prompt(七能力)
1. `inject(obj)`:材料注入唯一入口——强制 sort_keys、稳定缩进、统一围栏
   与标题定界。保证同数据 → 逐字节同片段(bf06 键序漂移事故的结构性解)。
   有序数据必须用 list;检测到数字串键的 dict 时告警(JSON round-trip 后
   int 键变 str,sort_keys 按字典序 1,10,11,…,2 排乱条目顺序)。
2. 严格渲染:prompts/*.md + {{槽位}};缺槽/多槽都报错;模板保持声明式,
   条件逻辑一律在 Python。
3. `Section`:具名条件段,值为 None 整段(含标题定界)不渲染,
   消灭"为 null 则跳过"式自然语言指令。
4. schema → 格式说明段:从 pydantic model(含 Field description)自动生成
   输出格式段与示例,prompt 与校验器单一真相源;"只输出 JSON 不解释"类
   固定措辞收编为唯一常量。
5. 裁剪:`clip(text, limit, boundary="line|sentence")` 安全边界截断;
   领域摘要函数不进库(约定:返回 str 即可作槽值)。
6. `render_items(items, format="json|bullets")`:列表渲染(table 无使用
   实证,暂缓;有第二个项目要再加)。
7. 修复段措辞常量(供 L2 引用,全库唯一一份)。

贯穿不变式:同输入 → 逐字节同输出(缓存键与确定性重放压在此上)。
公共 prompt 成分(措辞常量、schema 格式段模板、inject 围栏)以 golden
snapshot 测试锁字节;任何改动 = 全项目缓存换族,CHANGELOG 必须标注
"本版本导致缓存失效"。

截断三铁律:
1. 库永不默认截断;上限必须显式写在调用方。
2. 截断永不无声:prompt 内自动标注"(已截断:原文 N 字,注入前 M 字)",
   事件记入 sidecar `clips: [{slot, from, to}]`。
3. 修复环默认不截:continue 模式输出天然在对话历史;rebuild 模式
   echo 默认不限。

### L2 repair
- `repair_loop(caller, messages, validate, *, reminder=None,
  mode="rebuild"|"continue", max_repairs=2, sink=None, on_event=None, **params)`。
- **默认 rebuild**(三个源项目实证过的形态);continue(输出作 assistant
  消息接回对话再追加纠正指令,免回显、前缀缓存友好)标记为实验特性,
  经磁带机 + L3 统计对比跑赢后再转正——库 API 决策同样吃 RED/GREEN 纪律。
  已知风险:失败历史锚定坏输出;部分 OpenAI 兼容代理对
  assistant 消息 + response_format 组合不可靠。
- validate(raw) 抛 ValueError 即触发修复,返回值即结果——pydantic 解析、
  文本解析+门禁、组装+linter 全部套此签名。
- 固化学费:轮次编号进 prompt(防缓存不动点)、原样重交检测(stuck)、
  失败现场落盘(经显式 sink 参数,L2 不依赖 L3 的目录结构)、修复事件记录。
- `call_validated(caller, prompt, model_cls, extra_check=None, ...)`:
  JSON+pydantic 特化 = repair_loop + 确定性剥壳(extra_forbidden 机械删键、
  strict=False 控制字符容错)。

### 评估与进化(evals / optimize)
- `evals.py` 与 `optimize.py` 是库内的评估与进化层，不是独立产品；它们骑在
  L1 caller 之上，评委调用天然复用 L1 缓存。
- optimize 的 `(候选, 样例)` 判分状态文件只服务断点续跑，不绕过 L1；胜出文本
  不自动落盘，须经人工审阅后才进入 `prompts/`。
- `bench.py` 为同一流水线的结构切法生产变体×样例×种子的可归档证据，不裁决胜负，
  不自动接线；每个变体独立持有 caller，固定种子仍可复用 L1。

### L3 dag
- 自研 runtime 为底座(内部项目 771 行思路,干净重写),吸收:
  - redun `File`:外部文件声明为输入、内容进缓存键(prompts_hash 的推广);
  - Hamilton code_version:源码哈希忽略 docstring/注释;
  - LangGraph interrupt():检查点恢复语义(批准数据注回节点);
  - Metaflow foreach:一等公民动态 fan-out 原语(替代 f-string 拼节点名
    +循环闭包注册;episode_chain 的既知压力点)。
- L3 只缓存普通节点与 map/scan item，不提供任意流水线切片缓存；L1 调用缓存仍独立。
  `cache="auto"|"refresh"|"off"` 只决定 L3 读写，不改变 L1。
- 人工审批严格 run-local；一次执行只要实际调用过 `ctx.checkpoint()`，成功 artifact 也不写
  L3，防止批准结果经节点/item 缓存跨 run、挂载或 item 泄漏。条件分支未实际调用检查点时，
  仍保留节点声明的缓存策略。
- 缓存键 = 节点源码哈希 + helper/库哈希(source_dirs)+ 上游产物哈希
  + prompt 文件哈希 + 参数 + **kigumi prompt 相关模块哈希 + pydantic 版本**
  (库现在拥有 prompt 字节的生成权:inject 围栏/格式段/措辞常量变了,
  节点键必须失效,否则升级库后回放陈旧渲染的产物)。声明外部状态时只把
  `sha(external_fingerprint)` 作为可选 `external` 成分，原值不落 sidecar。普通依赖边声明
  `consumes` 时，`upstream:<dep>` 改为投影视图摘要，节点也只收到该 canonical 视图；未声明边
  仍按完整上游产物入键和传值。
- 文件物化是一等能力:artifact 带 `files: {相对路径: 内容}` 时落盘到
  项目目录,缓存命中也执行(subprocess 门禁靠磁盘文件工作;
  内部项目 _materialize 的收编硬依赖)。同一次 run 内，每条框架管理的项目相对
  物化路径只归一个生产者；符号链接与目标文件系统认定的大小写/Unicode 别名按同一目标
  认领，冲突在覆盖前失败；这是执行所有权，不是法律上的数据权利。
  另留 post-node hook。
- `Subgraph` 只保存静态多阶段声明，`Dag.mount()` 把它事务性挂入同一注册表；没有
  第二调度器或第二缓存。模型只选择内容，绝不返回可执行拓扑；运行时动态展开仅限既有
  map/scan，foreach 仍在注册期固化。
- 所有进入框架路径或扁平运行时名称的标识符都先过单段边界：普通节点与动态 item ID
  不得含 `@` 或路径分隔符，Subgraph 的 namespace/port/local 段还不得含 `.`；`run_id` 与
  检查点名必须是安全的单个非空文件系统成分。`.`/`..` 一律拒绝，挂载节点的
  `namespace.local` 与其检查点限定名只由已验证段组合生成。
- 检查点与 artifact 键的绑定声明式化(旧代码为硬编码)。
- 链式上文显式化为节点输入(禁止节点闭包携带跨迭代可变状态)。
- 新增:`diff <runA> <runB>`、缓存 GC(--keep-last N)、历史归档、
  人工检查点、token 预算。
- **止损线**:若第二个接入项目对 dag 提出 _materialize 级别的特化且
  post-node hook 接不住,dag 退回 vendored 模板,库只保 L0–L2。
  此线现在写下,将来才有勇气执行。

### 存储与容量
- `store.py` 是存储布局层，依赖方向固定为 `dag -> store`；它管理 run、节点缓存、
  归档、物化、审批与 GC，不能反向理解图或调度。
- `blobs.py` 是内容寻址二进制仓；`slots.py` 负责跨进程限流与自适应容量。

### enforce(守卫三环)
1. 注册环:@node 注册时 AST 检查节点函数体，是精确且权威的边界；循环内裸调
   `.call(`/`.llm(` 或节点体直接 raw-io，且无对应理由豁免 → 拒绝注册。
   **边界诚实声明**:运行环只覆盖 dag 项目;命令式项目
   主责在测试环与提交环。
2. 测试环:pytest 插件自动收集守卫检查 + 全模板 dry-render 测试;
   **新增豁免注释即告警**(waiver 是 agent 的自助逃生通道,必须留痕上报)。
3. 提交环:`kigumi init` 安装 git hook(core.hooksPath=.githooks,
   pre-commit 跑 `uv run kigumi guard --changed`);guard 输出含 waiver 清单。

source_dirs 级扫描只属于 dag check、测试环与提交环。raw-io 在这些环节用
`@*.node/map/scan/foreach(...)` 装饰器作启发式过滤，只扫匹配函数的最外层函数体：
这避免 helper 合法读文件的误报，但可能漏报；注册环仍是兜底。

### testing(pytest 插件,面向用户项目)
- 磁带机夹具:FakeTransport 录制/回放——真实模型的畸形输出(幻觉多余键、
  控制字符、原样重交)打真实修复环代码,消灭 fixture 喂养测试的结构性盲区。
- dry-render 全覆盖:prompts_dir 下每模板自动生成槽位可填满测试。
- L3 活体采样:@pytest.mark.live + 预算夹具(超限 skip 不烧钱)。
- `live` 是唯一的真实请求标记：插件激活时还要求 `KIGUMI_LIVE=1`，再与
  `skip_unless_env` 的凭证门叠加，形成显式双确认。
- 守卫即测试(三环之二)。

### 格式化/质量
复用 ruff(lint+format),脚手架交付 [tool.ruff] 预设;不自造工具。

### 契约层
可验证不变式的权威文本在 [docs/contracts/](docs/contracts/)；这里保留设计哲学、模块边界与
止损线，不复制可由测试锁定的实现细节。改动缓存键、确定性字节、守卫、检查点或保留语义时，
必须先更新对应契约和锁定测试，再记录发布影响。

### CLI
```
kigumi init          # 脚手架:prompts/ nodes/ tests/ + hooks + ruff/pytest 预设
kigumi guard [--changed]
kigumi doctor        # API key / 缓存目录 / hooks 体检
kigumi runs list / show <id>
kigumi diff <runA> <runB>
kigumi gc [--keep-last N]
kigumi render <node> --dry
```
`kigumi`(cli.py) 是不需要图注册表的项目运维入口：init、guard、doctor、render、runs、
approve、diff、gc。`dag.cli()` 是需要注册表的图检查入口：check、plan、graph、explain、
describe；两者不互相代替。
`kigumi init` 是 developing-ai-workflows skill 的落地点:新项目起步从
"复制模板"改为"跑 init"。

## 构建顺序

1. 仓库 + L0/L1 + artifacts ✅(commit b42bc1b)
   1.1 评审修正包:空响应耗尽即抛+拒缓存、resolved model 进缓存键、
       params 契约、Stdlib 超时、溯源先于记账、线程安全+in-flight 去重、
       litellm 降 extra、File 引用接口预留
2. prompt.py 七能力 + 截断三铁律 + golden snapshot(先于 repair:
   修复段措辞常量住在这里)
3. repair.py 双模式(默认 rebuild),验收关 = 在 worktree 里真实移植
   内部项目最难的一个循环(lint_fix,含跨轮事件累积与按错误类型
   定制指令),纸面对齐不算过关
4. enforce.py(纯函数形态 AST 检查器)+ testing.py(磁带机/dry-render);
   与 @node 的接线留给 P5,规格写明防止造假注册表
5. dag.py(foreach/diff/gc/物化/检查点绑定)+ cli.py + 运行环接线
6. 前置 6a:跨进程限流 + File 引用实现 + 并发路径验证;然后内部试点、
   既有项目收编、skill 更新(不只加红旗判例——skill
   "frameworks are banned" 不变量须重写为"物理层用库,编排逻辑必须
   一屏可读",否则遵循 skill 的 agent 会系统性抵触本库)

每步配套:旧项目 live 教训 → 本库 RED 用例,先红后绿。
每包审查新增机械动作:对照旧实现(llm_client.py / runtime.py /
progressive_annotation_pipeline.py)的 docstring 与注释提取能力清单逐条
勾兑——P1 曾带着 4 处对旧实现的退化过审,教训在此。

## 修订记录

- 2026-07-14 新增边上消费投影 `consumes`：普通节点、foreach、map/scan 共享依赖与
  Subgraph 本地依赖可声明实际消费视图；`CACHE_SCHEMA=3` 触发有意的完整 L3 缓存换族。

- 2026-07-14 发布 0.2.0：新增框架物化路径所有权、L3 `auto/refresh/off` 与外部状态
  指纹、静态可复用 Subgraph；`CACHE_SCHEMA=2` 触发有意的完整 L3 缓存换族。

- 2026-07-13 新增 segment bench：以假设和唯一现状对照约束结构探索，生成稳定的
  变体×样例×种子证据报告与按变体归集的调用成本；裁决和接线始终留在库外。

- 2026-07-13 新增 agent 可观测层：`inspect.py` 只读联接 run sidecar 与 L1 载荷，
  不导入 DAG、不发请求、不触碰缓存键成分；CLI 因而可完整追溯节点、map 项和调用证据。

- 2026-07-13 新增契约层：缓存键、确定性字节、守卫环与豁免、检查点审批、缓存与产物保留
  的可验证不变式统一收敛至 `docs/contracts/`；本设计文档只保留边界、哲学和止损线。

- 2026-07-13 libs 哈希对齐 code_version 原则：source_dirs 模块统一剥
  docstring/注释后按 AST 哈希，语法残破文件退回原文；此变更让 libs 成分
  全体换族，既有节点缓存一次性失效（greenfield 内无外部用户，直接执行）。

- 2026-07-13 第一轮整改：包结构补齐为实际 16 个模块；明确 evals/optimize、
  store/blobs/slots 的边界；守卫三环如实区分注册环与 source_dirs 启发式 raw-io
  扫描；记录双 CLI 分工与 `live` 标记统一为双确认门。

- 2026-07-10 独立评审(18 条)采纳合并:上述 params 契约、resolved model
  进键、空响应双闸、[tool.kigumi] 约定、node key 含库版本、物化一等能力、
  守卫边界诚实化、continue 降实验特性、dag 止损线、litellm 降 extra 等。
  驳回/缓办:table 渲染与 clip 滑窗(无使用实证,砍);多模态 File 引用
  实现与跨进程限流(归 6a,接口先行)。

## 命名记录

2026-07-10 定名 kigumi(木组)。曾用工作名 scriptkit(否决:不应绑定具体内容域)、
sunmao(PyPI 已占)。候选 dougong/rabbet/treenail 均可用,最终选 kigumi。
