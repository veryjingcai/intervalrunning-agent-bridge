# intervalrunning 智能体桥接服务

一个进程两副面孔：对手机跑 UDP 发现 + HTTP（端口 47823），对智能体跑 MCP（stdio）。两边共用 `data/store.json`。只用 Python 3.7+ 标准库，无需安装任何东西。

## 一、让手机连上（选一种）

**方式 A：常驻服务（推荐，手机随时可连）**

```
python bridge.py serve
```

窗口别关。这一条同时提供发现和 HTTP 服务。

**方式 B：依赖 MCP 会话**

什么都不用做。WorkBuddy 加载 MCP 后会自动拉起网络服务；会话结束就停。

端口固定在 47823，与手机 App 里写死的一致。电脑和手机必须在同一局域网，电脑防火墙要放行 47823（UDP + TCP）。

## 二、手机端操作

设置页 → 数据共享接口 → 打开开关 → 搜索设备 → 选中「WorkBuddy 智能体桥」→ 连接。

- 只读：能上传训练历史与课表数据（供智能体读取）
- 读写：额外能接收训练计划，以及智能体下发的课表数据并落库
- 课表读取权限两种档位都生效；课表写入、计划写入只在读写档生效，桥端与手机端双重校验。云端不参与，仅局域网自有设备。
- 连接后**自动同步**（打开开关/连接成功立即一次，之后每 15 秒），不需要在 App 里点按钮；
  智能体写入新计划时，App 会先把旧计划归档再生效，不会直接覆盖。
- 权限档位在 App 代码里配置，面板只读展示，不提供二次切换。

## 三、协议（v1）

手机与桥接服务之间走 HTTP + JSON，端口 47823。

| 方法 | 路径 | 鉴权 | 用途 |
|---|---|---|---|
| POST | `/v1/pair` | 无 | 连接：登记令牌、权限档位、设备标识与时区 |
| POST | `/v1/sessions` | 需令牌 | 上传训练历史（全量） |
| POST | `/v1/plan` | 需令牌 + 读写档 | 手机把当前计划推给桥端；必须带 `baseRevision`，期间智能体写过新计划则 409 |
| GET | `/v1/plan` | 需令牌 + 读写档 | 手机从桥端拉取计划 |
| POST | `/v1/workouts` | 需令牌 | 手机上传课表数据（两种档位均可）；必须带 `baseRevision`，旧版本会 409；已删除 id 不可复活 |
| GET | `/v1/workouts` | 需令牌 + 读写档 | 手机拉取智能体写入的课表数据并落库（写入权限） |
| GET | `/v1/ping` | 需令牌 | 探活：只验桥端可达且令牌有效，不要求写权限。回 `{code:"OK", clientId, permission}`，供手机设置页状态灯使用 |
| UDP | 47823 | 无 | 发现广播，手机发 `{"v":1,"type":"discover","client":"intervalrunning"}`，桥端单播回 `{"v":1,"type":"agent",...}` |

**请求体公共字段**

| 字段 | 说明 |
|---|---|
| `v` | 协议版本，固定 `1` |
| `client` | 客户端标识，固定 `intervalrunning` |
| `token` | 连接令牌（`/v1/pair` 用请求体带，其余用 `X-Agent-Token` 或 `Authorization: Bearer` 头） |
| `permission` | `READ_ONLY` 或 `READ_WRITE`，必须与配对记录一致，否则 403 `PERMISSION_MISMATCH` |
| `clientId` | 设备稳定标识，桥端按它隔离多台手机的记录 |
| `tzId` / `tzOffsetMinutes` | 手机时区，桥端按它把 `yyyy-MM-dd` 换算成 epoch 毫秒 |

**响应体公共字段**

| 字段 | 说明 |
|---|---|
| `code` | 业务码。成功为 `OK`，失败见下表；HTTP 状态码相同可能对应不同含义，**必须读 `code`** |
| `revision` | 智能体计划版本号（仅 `GET /v1/plan`）。智能体每写一次计划 +1，手机按它判断是否要重新拉取 |
| `phoneRevision` | 手机上传计划的独立计数；手机上传只推进它，不动 `revision`，避免手机自己反复拉取 |
| `source` / `writtenAt` | 计划最后由谁写（`agent` / `phone`）以及写入时间，见 `get_status` 的 `plans` |
| `plan` | 计划 JSON（仅 `GET /v1/plan`） |
| `permission` | 桥端为该令牌登记的档位（仅 `GET /v1/ping`） |

**探活的跨版本约定**：旧版桥端没有 `/v1/ping`，会回 404 `NOT_FOUND`。手机客户端读到该码后改探 `GET /v1/plan`，并把 `OK` / `NO_PLAN` / `PERMISSION_DENIED` 都视为「桥端活着」——这三个业务码能回来，就证明网络可达且令牌被认识。因此只升级 App 不重启桥端，状态灯依然可用。

**错误码**

| code | HTTP | 含义 |
|---|---|---|
| `OK` | 200 | 成功 |
| `UNAUTHORIZED` | 401 | 未配对或令牌失效 |
| `PERMISSION_DENIED` | 403 | 只读档做了写操作 |
| `PERMISSION_MISMATCH` | 403 | 请求体档位与配对记录不一致 |
| `PERMISSION_ESCALATION` | 403 | 同一令牌已登记为只读，不允许直接提权（需重新连接生成新令牌） |
| `TOKEN_CONFLICT` | 403 | 该令牌已绑定另一台设备 |
| `NO_PLAN` | 404 | 桥端还没有计划（**正常空态**，不是故障） |
| `NOT_FOUND` | 404 | 未知端点（**真故障**，不能当空态吞掉） |
| `BAD_REQUEST` | 400 | 请求体不合法 |
| `BAD_VERSION` | 400 | 协议版本不匹配 |
| `BAD_TOKEN` | 400 | 缺少连接令牌 |
| `BAD_PERMISSION` | 400 | 权限值非法 |
| `BAD_PLAN` | 400 | 计划格式不合法 |
| `CONFLICT` | 409 | 计划/课表版本前置条件不满足：期间已有新写入，先拉取再重试 |
| `BAD_SESSIONS` | 400 | sessions 字段不是数组 / 条目缺 id / startedAt 不是数字 / count 与条数不符 |

**设备隔离与提权规则**

- 训练记录按 `clientId::会话id` 两段索引：两台手机的会话 id 都从 1 开始，不隔离就会互相覆盖。
- 计划也按设备分槽存放，互不干扰。
- 同一个令牌只能绑定一台设备，也不能从只读提权成读写。提权需要**断开后重新连接**（手机会生成新令牌）。

## 四、智能体端（MCP 配置）

在本机 MCP 配置里加一项（stdio 方式）：

```json
{
  "mcpServers": {
    "intervalrunning": {
      "command": "python",
      "args": ["D:/Android/intervalrunning-agent-bridge/bridge.py", "mcp"]
    }
  }
}
```

配好后重启客户端，即可用这几个工具：

| 工具 | 用途 |
|---|---|
| `list_sessions` | 读手机上传的训练历史（支持条数、起始日期、排序、设备过滤） |
| `get_status` | 看配对状态、权限、记录数、上次同步、活跃设备，以及每台设备的计划名/条数/`revision`/`phoneRevision`/`source`/`writtenAt` |
| `set_plan` | 写计划。给 `steps` 就是计划内自定义训练，不依赖课表库；回执会带实际写入的 `clientId`，旧计划先留档 |
| `get_plan` | 读当前计划 |
| `set_plan_json` | 写 App 原生计划 JSON；entry 含 `steps` 而无 `customWorkout` 会自动补编码，无训练内容的 entry 会被拒绝 |
| `list_workouts` | 读取手机上传的课表数据（课表读取权限） |
| `set_workouts` | 写入课表数据（课表写入权限，手机需读写档）；可用 `name + steps` 自动编码。写入范围仅限「我的课表」：带 `isPreset` / `presetKey` 或命中本机内置预设的条目只另存副本，绝不覆盖预设 |
| `delete_workout` | 删除一套自定义课表（兼容单条；内置预设不可删） |
| `delete_workouts` | 批量原子删除自定义课表，支持 `expectedRevision` / `expectedCustomCount` 前置校验；重复提交幂等 |
| `keep_only_custom_workouts` | 原子清理到指定保留列表（如 21→10），版本/数量不符整批拒绝 |

典型用法：让智能体「读一下最近的训练历史，据此排一个四周计划」——它会调 `list_sessions` 再调 `set_plan`；手机接口开着就会自动同步落库。

`set_plan_json` 的 entry 原生格式：`id` / `date`(epoch 毫秒) / `hour` / `minute` / `workoutId` / `workoutName` / `customWorkout`(由 `encode_workout_snapshot()` 生成的 JSON 字符串)。若是 `set_plan` 风格的 `steps`，工具会自动编码成 `customWorkout`；`workoutId=0` 又没有任何训练内容的条目会被拒绝，避免手机显示「已删除」。回执会报「App 可解析 N 条」，`get_status` 也提供 `parseableEntries`。

`steps` 支持 `label`（步骤名，接口写入时统一截到 7 个字符）与 `cadenceSpm`（该段节拍器步频）。步频只在 App 的节拍器开启且模式为「跟随段落」时生效；「固定步频」模式使用 App 设置里的全局值，接口无法修改全局节拍器设置。

多台手机同时连着时，`set_plan` / `get_plan` / `list_sessions` 都可以带 `clientId` 指定设备；不带就作用于**当前活跃设备**（最近与桥端通信的那台）。没有任何设备连接时，不带 `clientId` 会直接报错，不再退回一个手机永远不读的 `default` 槽位。智能体写某台设备的计划也不会改变「当前活跃设备」。

## 五、命令

```
python bridge.py mcp       # MCP 模式（默认），同时起网络服务
python bridge.py serve     # 只跑网络服务，前台常驻
python bridge.py status    # 查看当前状态
python bridge.py selftest  # 自检，不改真实数据
python bridge.py mcp --store <其他路径>   # 换数据存储位置
```

## 六、验证脚本

除 `bridge.py selftest` 外，另有五个独立验证脚本，全部不碰真实数据：

| 脚本 | 验证什么 | 项数 |
|---|---|---|
| `bridge.py selftest` | 协议层自检：发现包、鉴权、校验、计划格式、并发写 | 49 |
| `keepalive_check.py` | 长连接复用：一次 403 之后同连接上的后续请求是否仍然正常 | 6 |
| `e2e_check.py` | 端到端联调：按 App 真实协议体走完「连接→上传→智能体写计划→手机拉取→手机回传」 | 26 |
| `migration_check.py` | 旧版 store.json 升级无损 | 15 |
| `concurrency_check.py` | 多进程并发写一条不丢 | 4 |
| `compat_check.py` | 新旧两侧单独升级时仍能互通 | 8 |

## 七、已补的洞（相对裸协议）

- 权限服务端反查：拉计划时按配对记录判 READ_WRITE，只读档直接 403，不信请求体
- 上传包 permission 与配对记录不一致 → 403
- **令牌绑定设备 + 禁止提权**：同一令牌不能改绑别的设备，也不能从只读直接升成读写
- **设备维度隔离**：训练记录按 `clientId::id` 存，计划按设备分槽，多台手机互不覆盖
- 按 session id 去重（upsert），重复上传不产生脏数据
- 统一错误码 JSON：`UNAUTHORIZED` / `PERMISSION_DENIED` / `PERMISSION_MISMATCH` / `PERMISSION_ESCALATION` / `TOKEN_CONFLICT` / `NO_PLAN` 等
- **入参严格校验**：`count` 与实际条数不符、条目缺 id、`startedAt` 不是数字，一律 400 拒绝，不进库
- **计划写入前按 App 的 `decodePlan` 规则校验**：`workoutId=0` 且无有效 `customWorkout` 的条目直接拒绝；`hour`/`minute` 越界、条目日期超出计划周期、空计划也拒绝
- **时区按手机口径**：计划日期换算用手机上报的 `tzId`/`tzOffsetMinutes`，不同时区的手机不会差出一整天
- `set_plan_json` 自动把 `steps` 编成 App 原生 `customWorkout`，回执与 `get_status` 报告 App 可解析条数
- 智能体写新计划前先把旧计划留档（`planArchives`），绝不直接删除/覆盖
- 存储原子写 + **跨进程文件锁**：「读-改-写」全程串行，MCP 与 HTTP 分进程运行时也不互相覆盖（修复前实测 75 条只剩 6 条）
- 原子替换带重试：Windows 上目标文件被短暂占用时不会直接失败
- 鉴权失败也会读完请求体：HTTP/1.1 长连接上残留的 body 不会污染下一个请求
- 脏数据（`startedAt` 是字符串等）不会让 `list_sessions` 排序崩溃

## 八、未做（需要时再加）

- HTTPS：当前 HTTP 明文，局域网内使用
- token 撤销/过期：目前配对后长期有效，靠 App 侧「断开」清理本地，桥侧无主动失效
- 计划冲突对话：手机上传带 `baseRevision`，期间智能体写过新计划会返回 409，手机先拉取再传；不做人工选择界面
- 计划按设备分槽后，MCP 侧不写 `clientId` 时作用于活跃设备；多设备场景建议显式指定
