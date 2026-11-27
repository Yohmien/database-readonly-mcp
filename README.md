# 数据库只读 MCP

提供 MySQL、MariaDB、PostgreSQL、SQL Server、Oracle 和 SQLite 的查询及结构探测。通过 `database_readonly` 服务提供通用 `db_*` 工具。服务使用 stdio，不启动监听端口。

## 快速开始

需要 Python 3.12。在仓库根目录执行：

```text
python -m venv .venv
# Windows
.venv\Scripts\python.exe -m pip install -r requirements.lock
# macOS / Linux
.venv/bin/python -m pip install -r requirements.txt
```

`requirements.lock` 保留 Windows 环境验证版本；其他系统使用 `requirements.txt`，无需安装 Windows 专属依赖。MySQL 镜像和 SQL 导入还需官方 `mysql`、`mysqldump` 客户端；通过 PATH 或 `DB_MCP_MYSQL_BIN` 指定其目录。MySQL Shell 可选，通过 PATH 或 `DB_MCP_MYSQLSH_BIN` 指定可执行文件。本仓库不分发第三方客户端二进制。

复制 `connections.example.json` 为 `connections.json`，仅保留自己需要的连接并修改地址。为其 `username_env`、`password_env` 设置环境变量，账号必须具备专用只读权限。SQLite 不需要账号，但数据库文件必须已存在。真实配置、备份、任务状态和导出文件均不纳入 Git。

通用 stdio MCP 客户端配置示例（路径使用自己的绝对路径）：

```json
{
  "mcpServers": {
    "database_readonly": {
      "command": "/path/to/database-readonly-mcp/.venv/bin/python",
      "args": ["/path/to/database-readonly-mcp/serve.py"],
      "env": {
        "DB_MCP_CONNECTIONS_FILE": "/path/to/database-readonly-mcp/connections.json"
      }
    }
  }
}
```

Windows 的 `command` 使用虚拟环境中的 `.venv/Scripts/python.exe`。客户端进程需要能读取所引用的凭据环境变量；不要把真实凭据提交到仓库。查询功能不需要镜像管理凭据；启用镜像或导入功能时再单独配置管理账号。

源码采用 [MIT License](LICENSE)。

## 工具

| 工具 | 用途 |
| --- | --- |
| `db_query` | 通用查询；`row_format` 选择行表示，返回列名、行数、截断标记和耗时 |
| `db_connections` | 返回已注册的连接目标，不连接数据库，不返回账号密码；`configError` 反映最近一次热重载失败原因 |
| `db_resolve` | 按连接名、别名、URL 或 Spring 项目解析唯一目标 |
| `db_schemas` | 列出 schema |
| `db_tables` | 按 schema 和名称片段查表/视图 |
| `db_describe` | 查询字段；`detail=full` 追加主键、外键、索引、唯一约束 |
| `db_explain` | 获取不执行查询的执行计划；仅 MySQL/MariaDB/PostgreSQL/SQLite |
| `db_sql_check` | 指定 `sql/dialect`，复用现有开源 SQLGlot 离线解析；返回语法错误、未支持语法及风险提示，不执行 SQL |
| `db_mirror` | 唯一镜像操作入口，同来源同引擎建库、清空、结构/数据初始化、SQL 执行、受控测试进程与删除；来源始终只读 |
| `db_task_result` | 读取超过 10 秒的后台任务：默认只返回状态、最近进度和结果摘要；可按行、增量或全量读取脱敏日志，显式获取完整结果 |
| `db_sql_import` | 用户提供 SQL 文件初始化数据库或测试库时直接导入新库；库名缺省取文件名，可用 `database_name` 指定；按 INSERT 语句分块并行写入，支持多 GB 文件、验证查询和受控清理；导入库默认保留，超过 1 GB 的删除需用户确认后传 `confirm_large_drop=true` |
| `db_script_test` | 自动准备并清理镜像，用真实 MySQL 客户端执行脚本两遍，按需执行 `verify_sql` 并核对前后结果；适用于 `DELIMITER`、存储过程、函数和触发器等客户端脚本 |
| `db_test_dataset` | 按业务条件剪枝并导出便携数据集，或直接载入唯一镜像；支持补充小型依赖表、载入后 SQL、验证查询及可重复的剪枝缺口增量闭环 |

## 镜像测试入口

普通 SQL 优先 `db_sql_check` 和必要只读核验，非必要不建镜像；高危写入、删除、破坏性 DDL 或明确执行疑点时使用 `db_mirror`。静态解析通过不代表对象存在、幂等或运行正确，SQLGlot 回退为未解析命令时返回 `incomplete`。

SQLGlot 不解析 MySQL 客户端的 `DELIMITER` 包装和存储对象定义。`db_sql_check` 识别到 `DELIMITER`、存储过程、函数、触发器或事件时返回 `status=incomplete` 与 `recommendedTool=db_script_test`。这种情况使用 `db_script_test`，不要绕过 MCP 调用数据库客户端。

`db_script_test` 使用 `prepare=empty/init_empty/clone` 准备唯一镜像，执行主脚本两次，并在每次主脚本后执行可选的 `verify_sql`，比较两次脚本输出和验证结果。无论成功或失败都会 `drop` 镜像并核验不存在；已有镜像时拒绝启动，清理失败时明确返回未完成。`init_empty/clone` 必须用 `tables` 显式限定基础表。

## 测试数据剪枝与补充

`db_test_dataset` 必须提供 `tables` 和非空 `prune`。每张所选表必须且只能归入 `prune` 或 `supplement_tables`：

- `prune` 是表名到单条 WHERE 条件的映射。MySQL Shell 在源端导出时直接过滤，避免先生成全表再删除。
- `supplement_tables` 是完整复制的小型依赖表，例如当前场景需要的系统字典、设备映射和基础配置。
- `supplement_sql` 在数据载入镜像后执行，可添加缺失的业务样本，支持 MySQL 客户端多语句和 `DELIMITER`。
- `verify_sql` 在补充完成后执行一次，输出与镜像一起返回。
- `threads` 控制 MySQL Shell 并行度，`bytes_per_chunk` 控制分块大小；默认 `4` 线程、`8M`，可在数据库连接能力允许时提高到 `8` 或 `16` 线程来缩短测试数据集导入时间。

应用启动或完整回归建议使用 `schema_mode=all`。工具只复制 `prune`/`supplement_tables` 指定的数据，其余基础表自动使用 `1=0` 保留 DDL。后续发现缺表或数据范围不足时，通过 `extend` 增量闭环处理，不重新生成全量数据包。

MCP 使用过程中直接调用工具完成查询和写入，不为每条 SQL 新建 Python/PowerShell 文件。多个接口场景复用同一镜像，只有进程级集成测试使用一次性的 `run_test` 入口，并在同一入口内完成查询、断言和证据输出。

补充表是受控例外。`schema_mode=all` 已保证所有结构存在，因此只有确实需要新增业务记录时才用 `extend`。每轮应先集中收集当前已确认的全部缺口，合并成一个 extension，避免逐表调用。

### 剪枝缺口闭环

`db_test_dataset` 将 `extend` 作为可重复的剪枝修复步骤，而不是一次性的补救：

1. 使用 `db_query`、`db_describe` 和 `db_explain` 在来源库只读交叉验证当前业务键，记录 SQL 条件、原始行、预期事实和实际事实。
2. 镜像结果与源库不一致时，先判断差异是否来自被 `1=0` 排除、`prune` 条件过窄、`supplement_tables` 不足或缺少基础配置。只有能够归因到剪枝数据缺口时才执行 `extend`。
3. `extend` 只导出本轮新增缺口，追加到当前唯一镜像和同一数据包的 `extensions/NNN`，不重建全量数据集，也不创建平行数据包。随后立即用 `verify_sql` 或同一测试入口在镜像隔离复验。
4. 复验仍失败时，回到步骤 1 重新定位源库事实和残余缺口。确认仍是剪枝缺口后执行下一轮 `extend`，如此循环，直到镜像事实与源库一致并完成隔离验证，或证据排除了剪枝原因。
5. 不能为了尽快结束循环而跳过源库交叉验证，也不能把未归因的失败直接当作业务代码缺陷。MCP 负责生成可核验的数据集和镜像事实，行为缺陷的判断与后续处理不属于数据准备工具。

循环的退出条件只有两个：镜像已补齐并可解释与源库的全部差异，或已有证据表明失败与剪枝数据无关。任一条件成立后仍需完成当前测试，并在交付前 `drop` 镜像。

MySQL Shell 因权限或本机运行异常失败时，工具自动回退到官方 `mysqldump --where` 的按表并行导出，并记录 `archive.engine` 与 `fallbackReason`。Windows 子进程保留 `SystemDrive`、过滤不存在的 PATH 项，并在调用前创建导出包父目录，避免 MySQL Shell 把本地化路径错误按 UTF-8 解码后表现为 `UnicodeDecodeError`。

`action=prepare` 会拒绝已有镜像，剪枝导出后载入唯一镜像，保留现场供测试，测试完成后必须通过 `db_mirror drop` 清理。`action=export` 将结果导出到新的绝对 `output_dir`，目录包含 `dump/`、`mcp-dataset.json` 和可选的 `supplement.sql`。`action=load` 从该目录恢复镜像时也执行相同的来源、文件边界和剪枝元数据核验。`action=extend` 只导出本轮确认的新增缺口，追加到现有镜像和同一数据包的 `extensions/NNN`，后续 `load` 会顺序重放。每轮记录 `status`、`verificationStatus` 和失败原因，`status` 为 `passed`、`loaded` 或 `failed`；加载失败或隔离验证失败的轮次也会落盘，下一次 `extend` 继续使用新编号，形成可审计的补缺循环。

示例：

```json
{
  "action": "prepare",
  "database": "warehouse",
  "tables": ["order_header", "order_detail", "lookup_type"],
  "prune": {
    "order_header": "order_no LIKE 'DEMO-001%'",
    "order_detail": "order_no LIKE 'DEMO-001%'"
  },
  "supplement_tables": ["lookup_type"],
  "verify_sql": "SELECT COUNT(*) AS n FROM order_header",
  "threads": 8,
  "bytes_per_chunk": "8M"
}
```

`db_mirror` 沿用来源路由参数，`action` 支持 `status/create/clear/init_empty/clone/execute/run_test/drop`；`execute` 传 `sql`，`init_empty/clone` 可传 `tables`。`run_test` 传 `command`（字符串数组）和可选 `cwd`（存在的绝对目录，默认 MCP 目录），在镜像生命周期内启动受控子进程。`timeout_seconds` 对 `run_test` 可选，省略时不限制测试进程时长；其他镜像动作默认 60 秒。实际 MySQL 实例 `server_uuid`、方言与规范库名产生稳定 `sourceId` 与 `dbm<哈希>` 库名，不同地址/连接别名指向同一来源时复用一个镜像。不允许调用者自选写入库或更换端点。首版支持 MySQL，直接在来源引擎新建库，不需要 Docker。需要固定镜像库名的单镜像测试可在 MCP 环境配置 `DB_MCP_MIRROR_ALLOW_NAMED=1` 后传 `mirror_name`（小写字母/数字/下划线，不能与来源库同名，且不参与 `status` 的默认绑定查询）。

## 长耗时调用与任务日志

所有数据库 MCP 工具共用 10 秒快返：10 秒内完成时直接返回原结果；超过 10 秒时返回 `status=running`、`taskId`、`logPath` 和 `readTool=db_task_result`，后台继续执行。MCP 响应超时不代表执行失败，不得因超时重复发起同一调用。

超过 10 秒的返回带 `estimate`：`remainingSeconds`、`totalSeconds`、`finishAt`、`source`（`progress` 或 `reference`）和 `basis`。`db_sql_import` 按参考吞吐估算，并用已完成字节修正；其他工具按已完成任务或字节外推。导入参考基准是任务 `import-benchmark`：6.6 GB 文件、977 个数据任务、177 张表、511.6 秒（约 12.9 MB/s）。

`db_task_result` 默认只返回任务状态、最近进度、结果摘要和异常摘要，不整包返回结果或日志：

- `log_mode=tail` 省略 `offset` 时读取末尾 `max_bytes`；传 `offset` 时从该字节位置增量续读；`log_mode=full` 读取完整日志。
- `line_start` 与 `line_end` 按 1 起始行号读取对应日志行；只传 `line_start` 时读取该行。
- `include_result=true` 才返回完整结果，仅在摘要不足时使用。
- 执行异常时，`resultSummary.error` 与顶层 `error` 保留完整的安全异常记录（类型、错误码、errno、消息）；失败任务同时保留完整 `stdout`/`stderr`，日志写入 `event=error`。

日志默认写入服务目录下的 `run-logs/`，可用 `DB_MCP_RUN_LOG_DIR` 指定；单任务日志默认上限 32 MiB，可用 `DB_MCP_RUN_LOG_MAX_BYTES` 调整。后台任务运行在 MCP 进程内，服务重启后旧任务标记 `interrupted`；Windows Job Object 会随服务退出清理测试子进程。

## SQL 文件导入

用户提供 MySQL SQL 文件用于初始化数据库或测试库时，直接使用 `db_sql_import action=import`，不要先用 `rg`、文本读取或 `db_sql_check` 拆解该文件，也不在来源库直接执行其中的 SQL。该规则适用于小文件、仅含 DDL 和普通顺序 SQL；用户明确要求审查 SQL 内容时按审查任务处理，不执行导入。

`db_sql_import` 将 SQL 文件导入新数据库，数据库名缺省取文件名去掉 `.sql` 后的部分，可用 `database_name` 指定；`action=status/drop` 也可只用 `database_name` 定位。`action=import` 要求绝对 `sql_path`，目标库已存在即拒绝；`threads` 支持 1..16，`chunk_mb` 支持 1..1024，多 GB 文件建议使用 4..8 个线程和 64 MB 分块。导入后可传 `verify_sql` 在新库权限范围内核对数据。

工具按 SQL dump 的表结构标记切分文件：先逐表执行建表、清表与临时视图占位结构，再按完整 `INSERT` 语句分块并行导入，最后执行索引/最终视图等后缀和存储过程等对象。文件不会被整体读入内存，客户端通过有界字节流按区间读取。没有可识别结构标记的普通 SQL 文件退化为单流顺序导入。

导入使用仅授权新库的临时账号，root 管理凭据只执行建库、核验和删除等固定管理语句。导入失败会自动删除新库；成功后保留该临时账号以支持 `SQL SECURITY DEFINER` 对象。`action=status` 返回库是否存在及是否归本工具登记，`action=drop` 只删除本工具登记的库并清理临时账号，重复 `drop` 保持幂等。

导入库成功后默认保留，供后续查询和验证使用；测试结束不自动删除。只有用户明确要求清理时才执行 `action=drop`。删除前按 `information_schema` 统计实际数据量，超过 1 GB（1073741824 字节）时必须先征得用户同意，再传 `confirm_large_drop=true`，否则工具拒绝删除。镜像库（无论来自导入库还是既有库）在测试结束后必须立即 `drop` 并确认 `exists=false`，不得保留。

所有镜像动作均经过 MCP。调用者不得直连 MySQL、调用内部 API 或运行客户端代替本入口。内部复用 SQLAlchemy/PyMySQL、官方 `mysqldump/mysql`、filelock 与 keyring，不自写导出恢复、SQL 拆分或文件锁引擎。

镜像管理账号可使用用户提供的 root，凭据通过系统 keyring 服务 `database-readonly-mcp`、条目 `root@<host>:<port>` 获取（回环地址统一为 localhost），或由 MCP 环境 `DB_MCP_MIRROR_ADMIN_USER/DB_MCP_MIRROR_ADMIN_PASSWORD` 注入。来源只读连接仍保持不变。root 只执行固定管理语句；待测 SQL 在仅授权当前镜像库的随机临时账号下运行，支持 MySQL 客户端 DELIMITER、多语句与脚本内会话变量；拒绝强制角色及其他越界授权。账号随镜像保留以支持 DEFINER 对象，每次调用轮换密码并清理残留连接，重建/删除时先删镜像对象再删账号；中断后从登记恢复清理。

客户端目录通过 `DB_MCP_MYSQL_BIN` 配置；否则查 PATH 与 Windows 标准 MySQL 8.0 安装路径。`--no-defaults` 与临时 `MYSQL_TEST_LOGIN_FILE` 阻止本机客户端配置覆盖凭据。状态登记文件默认 `mirror-state.json`，可用 `DB_MCP_MIRROR_STATE_FILE` 指定；其中没有密码，不应手工改写。未登记的同名库拒绝接管。MySQL TLS 选项映射到官方客户端，不能静默丢弃安全参数。

`create` 建空库；`clear` 清空基础表数据；`init_empty` 重建选中来源表结构、不导入记录；`clone` 重建表结构与数据。保留来源默认字符集/排序规则。导出不包含视图、触发器、例程、事件，不全文替换跨库引用；依赖这些对象的测试必须披露覆盖限制。导出上限 128 MiB，可缩小 `tables`；输出上限 1 MB 并返回 `truncated`。非事务表或并行 DDL 不承诺快照一致性。

大库测试先确定用例涉及的表和依赖表。所选表的数据量可控时，显式传入 `tables` 执行 `clone`。涉及大业务表时，用 `init_empty` 建立这些表的结构，再按订单号、主键或必要时间窗只读查询关联记录，通过 `execute` 导入满足测试所需的样本。大库省略 `tables` 执行 `clone` 会导出全部可见基础表，容易达到导出上限或超时，应避免用于日常测试。

`mysqldump` 的输出经临时文件以原始字节交给 `mysql`，Python 无需读取和重新编码整份 SQL。来源导出完成并通过大小检查后才重建镜像。

对静止的测试数据，可显式传 `{"action":"clone","clone_engine":"mysqlsh","tables":["order_header","order_detail"]}` 使用 MySQL Shell 四线程导出与导入。需要可用的 MySQL Shell 以及满足 Shell 权限检查的来源只读账号。MySQL Shell 8.4 在 MySQL 8.0 上需要读取 `mysql.*` 以检查角色。MCP 使用镜像管理账号检查目标实例的 `local_infile`，为 `OFF` 时执行 `SET PERSIST local_infile=ON` 并复核；不支持持久化变量的托管实例需由服务提供方配置。并行导出各线程独立读取，适用于导出期间没有业务写入和 DDL 的来源。常规在线业务数据使用默认的 `mysqldump` 一致性快照。MCP 通过 `DB_MCP_MYSQLSH_BIN`（可执行文件的完整路径）、PATH 或随服务安装的 Windows 客户端查找 `mysqlsh`。并行归档大小上限为 128 MiB，条件不满足时可用默认方式克隆所选表，或用 `init_empty` 加载必要样本。

幂等测试后（含失败）必须 `drop`，再 `status` 确认 `exists=false` 才交付；清理失败要报告未完成。DDL 与客户端脚本可能部分提交，不承诺整体回滚。

`run_test` 只用于受信任的本次测试脚本，不建设通用执行平台，也不是 OS 级沙箱；子进程仍以 MCP 运行用户身份执行，调用者只能运行受信任脚本。子进程环境由 MCP 生成：`DB_MCP_TEST_HOST/PORT/DATABASE/USER/PASSWORD/URL/JDBC_URL`，以及 `SPRING_DATASOURCE_URL/USERNAME/PASSWORD` 和 `SPRING_DATASOURCE_DRUID_MASTER_*`；不注入镜像管理账号、`MYSQL_PASS` 或其他父进程凭据。命令参数不得包含测试密码，禁止通过 `cmd/powershell/sh` 等 shell 启动；stdout/stderr 在流式读取时先脱敏，再写入任务日志和 64 KiB 有界响应缓冲，原始未脱敏输出不落盘。Windows 使用挂起启动、Job Object 绑定后再恢复；绑定或恢复失败时在测试负载执行前拒绝，不返回 passed。POSIX 使用独立进程组；超时、正常结束或失败时清理测试连接和仍存活的子进程。TLS 镜像连接暂不支持，需先扩展明确的证书映射，不降级为 `useSSL=false` 后静默运行。

## 上下文开销

每行只返回一种表示，避免同一结果重复。`row_format` 默认 `objects`（列名映射）；列名重复或按位置读取时用 `arrays`，此时只返回 `values`；需要同时比对两种表示才用 `both`。

`db_query` 默认 `limit=100`，需要更多行时显式提高，仍受连接配置约束。`db_describe` 默认只返回 `columns`，需要键、索引和约束时传 `detail=full`。`db_connections` 和 `db_resolve` 只在目标未知或需要核对匹配结果时调用，查询与元数据工具自身完成目标解析。

## 连接与自动匹配

实际配置为服务目录下的 `connections.json`，也可由 `DB_MCP_CONNECTIONS_FILE` 指定。参考 `connections.example.json`，只添加已有的专用只读连接；示例地址和账号环境变量不会自动产生可用连接。`url` 禁止包含账号密码，凭据通过 `username_env`、`password_env` 或 `url_env` 引用进程环境。

注册 URL 的驱动参数使用白名单，拒绝 `init_command`、`odbc_connect` 和主机/数据库/凭据覆盖参数，避免核验前执行 SQL 或实际连接偏离匹配目标。

所有需要数据库的工具支持 `connection`、`datasource_url`、`project`、`profile`、`database`、`dialect`。选择规则为硬条件交集，只有一个连接匹配时执行：

1. 连接名/别名和数据库类型限定候选。
2. `datasource_url` 支持 SQLAlchemy URL 与常用 JDBC URL，仅提取类型、主机、端口、数据库或 Oracle service。应用账号密码不用于连接。
3. `project` 读取项目根目录、`config/`、`src/main/resources/` 的 `application.yml`、`.yaml`、`.properties`；基础文件指定的 active/default Profile 会应用对应文件。支持 YAML 多文档及 Druid master。多模块、多 datasource 导致多个目标时返回歧义，必须补充明确 URL。
4. 查询目标由显式 `database`、连接 URL 或 Spring 项目配置确定；全限定表名不替代目标参数。仅注册为跨库连接的 MySQL/MariaDB 条目可以动态选择库；条目指定数据库时固定该库，可用 `allowed_databases` 进一步限制跨库范围。
5. 任意显式条件冲突、未知目标、多个候选、未解析占位符、复杂 Profile 组合或外部配置导入均拒绝，不尝试其他数据库。

例如：

```json
{"project":"/path/to/spring-project","sql":"SELECT COUNT(*) AS n FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=:schema","params":{"schema":"已确认的目标库"},"limit":10}
```

`project` 用于目标解析，不建立新连接，不枚举注册表以外的数据库。新数据库需要先注册目标和专用只读账号。

## 只读保护

业务 SQL 必须通过方言 AST 检查。拒绝多语句、写入/DDL/权限修改、写 CTE、嵌套锁定、`SELECT INTO`、文件输出、可执行注释、未知函数及 schema 限定函数；解析失败即拒绝，不回退到前缀放行。MySQL 元数据采用有限语法，支持限定 `DESCRIBE` 和 `SHOW COLUMNS`。`EXPLAIN ANALYZE` 被拒绝，Oracle 的 `EXPLAIN PLAN` 可能写入 `PLAN_TABLE`，因此不开放。

来源查询每次重新核验账号、建立独立连接，不复用上次权限结果或可变选库状态。只读模式建立失败直接报错；操作结束回滚并关闭连接。数据库账号权限是主要边界，SQL 检查与事务限制提供额外保护。镜像管理的 root 凭据与来源连接分离，仅由 `db_mirror` 使用。

| 类型 | 账号核验与连接保护 | 查询限时 |
| --- | --- | --- |
| MySQL/MariaDB | `SHOW GRANTS` 仅接受 USAGE/SELECT/SHOW DATABASES/SHOW VIEW；角色授权或未知权限拒绝。只读事务，禁止开启写配置 | MAX_EXECUTION_TIME / max_statement_time + 驱动网络超时 |
| PostgreSQL | 拒绝高权限角色、对象所有权、表/列写权限、schema/database CREATE 和非系统例程 EXECUTE；拒绝自定义类型及运算符。连接默认只读，操作事务只读 | statement_timeout + connect_timeout |
| SQL Server | 拒绝管理员角色及服务器、数据库、对象、schema、列级写/执行权限；拒绝更新锁/排他锁提示。没有通用的强制只读事务，必须通过权限核验 | ODBC 查询和连接超时 |
| Oracle | 拒绝对象所有者、危险系统权限及对象/列/角色/PUBLIC 危险授权；严格核验可能拒绝常规账号，需要管理员准备受限环境。只读事务 | 驱动 call_timeout / tcp_connect_timeout |
| SQLite | 仅打开已有文件，URI mode=ro、query_only、禁用扩展、trusted_schema=OFF；业务查询再加 authorizer | progress handler 截止时间 |

`db_query` 默认返回 100 行，可用 `limit` 调整，但不能超过 `max_rows`（最大 10000）。结构类工具默认最多 100 项。每条语句默认 15 秒，配置上限 60 秒。达到行数上限时读取一条探测行，返回 `truncated=true`。执行账号核验和多项结构探测会包含多条内部只读语句；超时分别作用于每条语句/驱动调用，不是整个 MCP 请求的总时限。

仅接受标量命名参数（`:name`）。驱动错误只输出错误类别/代码，不输出凭据、原 SQL 或参数。Decimal 输出字符串，日期时间输出 ISO 字符串，二进制输出 base64 对象。

只读事务不等于任意存储函数、视图或数据库扩展都无外部副作用。未知函数被拒绝；新增函数必须验证具体方言语义。数据库对象和服务端扩展仍由管理员管理。

## 安装与验证

在隔离 Python 3.12 环境安装 `requirements.lock`，运行 `python serve.py`。连接文件默认位于 `serve.py` 同级；连接的增删改通过 `manage_connections.py` 统一注册入口完成，变更在下一次工具调用自动热重载，无需重连 MCP。

```text
python -m pip install -r requirements.lock
python -m unittest discover -s tests -v
```

`tests/live_smoke.py` 通过实际 stdio MCP 子进程验证调用者配置的只读 MySQL，仅读取权限、常量和结构。手动运行前设置 `DB_MCP_SMOKE_PROJECT` 为 Spring 项目路径，设置 `PYTHONPATH` 为仓库根目录，并提供 `DB_MCP_CONNECTIONS_FILE` 及凭据环境变量；可用 `DB_MCP_SMOKE_TABLE_PATTERN` 筛选表。该脚本需要目标库至少有 6 张可见表以验证分页。`tests/mirror_smoke.py` 和 `tests/run_test_smoke.py` 还需要 `DB_MCP_SMOKE_TABLE` 及镜像管理凭据，会创建和清理镜像，仅应对自己授权的实例运行。SQLite 集成测试只对临时测试文件创建夹具，随后验证业务路径无法写入；不向实际业务数据库写数据。

SQL Server 需要匹配 URL 的 ODBC 驱动。既有验证覆盖 MySQL 和 SQLite；PostgreSQL、MariaDB、Oracle、SQL Server 尚未完成实库验证。历史性能记录仅作参考，不代表新环境的验收结果。

## 参考

- [sql-alchemy-mcp，固定版本 7d6f11c](https://github.com/alexxonline/sql-alchemy-mcp/tree/7d6f11cfe0cb210baf4e37286e66971b900e976d)：借鉴驱动抽象、元数据工具、参数绑定和返回限额。
- [jdbc-mcp-server，固定版本 c2c4362](https://github.com/igorolv/jdbc-mcp-server/tree/c2c4362121b4b51868add386f4916346aacc38a9)：借鉴命名连接、分层只读保护和按能力开放工具。

实现独立编写，不采用前缀放行、解析失败自动降级、可关闭只读保护或执行有写入副作用的计划工具。

## 1.2 修复

- allowed_databases 同时约束 SQL 引用、显式目标与元数据 schema；禁止未选库绕过。
- db_tables/db_schemas 增加 offset，响应 nextOffset=null 表示最后一页；分页期间目标或过滤条件保持一致。
- 重复列的对象键自动去重，并用 rowKeys 记录；columns 保留原始列名，values 保留位置值。
- 补充 UNIX_TIMESTAMP、FROM_UNIXTIME、FIELD、FIND_IN_SET、JSON_ARRAYAGG 的安全函数及解析器名称。
- 保留 active Profile 自动匹配，未声明环境时读取 application-default 文件。

## 1.3 热重载与统一注册入口

- 服务在每次工具调用前检查连接文件签名（mtime+size），变更时用与启动相同的 `load_connections` 重新加载；六种数据库类型走同一注册与校验路径，热重载与类型无关。运行中的服务无需重连即可看到新增或修改的连接。
- 重载失败不切换：文件缺失、JSON 损坏或校验失败时保留已加载配置继续服务，错误记录到 `db_connections` 的 `configError`，下一次调用自动重试，修复后错误自动清除。
- 连接的增删改统一走 `manage_connections.py`，不直接编辑 `connections.json`：写入前用服务同一套校验预检（失败回滚不落盘）、临时文件 + `os.replace` 原子写入、每次写前备份到连接文件同级 `backups/`、成功写入后保存 `*.last-good.json`；`restore` 从最近一次成功写入恢复；`test --connect` 用与查询工具相同的 Backend 真实核验账号与只读模式。
- 凭据环境变量的值在进程启动时由客户端 harness 注入：URL、别名、限额、新增连接等变更热生效；`username_env`/`password_env` 引用的变量名对服务可见之前（harness env 未配置），对应连接不可用，`configError` 与注册入口的警告都会提示原因，补齐后自动生效。
- 集成验证：`tests/test_hot_reload.py` 覆盖运行中注册（sqlite 实连 + mysql 注册）、损坏文件保持旧配置、`restore` 恢复；`tests/test_manage_connections.py` 覆盖原子写入、校验回滚、备份与恢复。

## 1.6.2 SQL 导入

- 新增 `db_sql_import`，以 SQL 文件名创建新库，按表结构和完整 INSERT 语句切分后并行导入。
- 导入失败自动删除新库；成功后可 `status` 核验并用 `drop` 清理，临时账号随库保留以支持 DEFINER 对象。
- 5.6 GB、139 表、3 视图、1 存储过程的 HeidiSQL dump 实测通过，8 线程、64 MB 分块约 10.5 分钟。

## 1.7.0 长耗时任务快返

- 所有数据库 MCP 工具在 10 秒内完成时直接返回；超过时返回 `taskId` 与日志位置，后台继续执行，避免客户端 50 秒响应上限造成执行状态丢失。
- 新增 `db_task_result`，默认只返回状态、最近进度和结果摘要；支持按行、增量或全量读取脱敏日志，并可用 `include_result=true` 显式获取完整结果。
- `db_mirror run_test` 省略 `timeout_seconds` 时不限制测试进程时长；日志记录启动、进度心跳、stdout/stderr、异常和最终状态。
- 服务重启后未完成任务标记 `interrupted`；异常摘要保留完整安全错误信息，日志写入 `event=error`。
