# MCP 镜像测试

来源数据库继续使用已注册只读账号。所有镜像动作与 SQL 静态检查由 MCP 工具提供，调用者不得直连数据库、执行 Docker/客户端命令或调用内部 Python API 操作镜像。

## 行为与验收

- `db_sql_check` 复用 SQLGlot，离线解析指定方言、多语句及危险操作；未支持语法不能报告检查通过。静态检查不证明对象存在、幂等性或运行结果。
- SQLGlot 无法解析 MySQL `DELIMITER` 客户端包装及存储过程、函数、触发器、事件定义。此类语法返回 `incomplete` 和 `recommendedTool=db_script_test`，不把解析失败误报为 SQL 语法错误。
- `db_mirror` 是唯一镜像入口，支持 `status/create/clear/init_empty/clone/execute/run_test/drop`。来源按已有连接解析确定；以实际 server_uuid、方言与规范库名绑定唯一镜像，同一来源的不同连接别名/地址复用一个镜像。所有数据库 MCP 工具共用 10 秒快返，超时返回 `taskId`/日志位置，后台继续执行。
- `db_script_test` 是完整脚本验证入口。它以 `prepare=empty/init_empty/clone` 准备镜像，用真实 MySQL 客户端执行主脚本两遍，并在每遍后执行可选 `verify_sql`；比较输出后无论成功或失败都删除镜像并核验不存在。已有镜像时拒绝执行，`init_empty/clone` 必须显式传 `tables`。
- `db_test_dataset` 在源端直接剪枝并并行导出。`tables` 中的每张表必须且只能归入非空 `prune` 或 `supplement_tables`；`prune` 使用 MySQL Shell `where`，`supplement_tables` 完整复制小型依赖表。`prepare` 将结果载入唯一镜像后执行可选 `supplement_sql` 与 `verify_sql`，保留现场供测试。`export` 生成含 `dump/`、`mcp-dataset.json` 和 `supplement.sql` 的便携目录，`load` 核验来源、路径边界和清单后恢复镜像。`extend` 导出本轮确认的缺口，追加到现有镜像和同一包的 `extensions/NNN`，并由后续 `load` 顺序重放。
- `schema_mode=all` 用于应用启动和全链路回归。它保留全部基础表 DDL，只复制 `prune`/`supplement_tables` 指定数据，其余表由工具自动加入 `WHERE 1=0`。初始数据包应先集中分析依赖，不得因单张缺表重新生成全量包；后续数据缺口使用 `extend` 增量补充。
- 默认 `threads=4`、`bytes_per_chunk=8M`。连接与实例允许时可提高并行度，但应先确保业务过滤条件能利用索引，避免为追求线程数对源端造成重复全表扫描。
- MySQL Shell 缺少二进制日志元数据权限或本机执行失败时，自动回退到官方 `mysqldump --where` 按表并行导出。结果记录实际 `engine` 与 `fallbackReason`，不提高来源账号权限。
- Windows 子进程环境保留 `SYSTEMDRIVE` 并移除不存在的 PATH 项。导出包在调用 MySQL Shell 前创建父目录，避免中文 Windows 的本地化路径错误被 UTF-8 解码后误报为 `UnicodeDecodeError`。
- 首版仅 MySQL。在来源所在的同一引擎建立独立库，复用本机官方 `mysqldump/mysql` 与现有 SQLAlchemy/PyMySQL；filelock 提供跨进程操作锁。不自写 SQL 拆分、导出、恢复或锁实现，不引入 Docker。
- 来源使用已核验只读账号导出。用户提供 root 管理凭据，keyring 保存到系统凭据存储；root 只执行固定生命周期语句。待测 SQL 以仅获当前镜像库权限的临时账号执行，拒绝强制角色；账号随镜像保留以支持 DEFINER 对象，调用间轮换密码并关闭残留会话，重建/删除先删库再删账号；中断登记用于恢复清理。镜像工具不接受另一端点，越库 SQL 由权限拒绝。凭据不出现在参数和结果中。
- `create` 新增空库；`clear` 清空表数据保留结构；`init_empty` 重建来源表结构不导入记录；`clone` 重建来源表结构及数据；`execute` 执行原始 SQL（支持客户端 DELIMITER）；`run_test` 在镜像生命周期内启动受控测试进程；`drop` 删除所属镜像库，核实不存在。命名与来源登记必须匹配，不能接管同名其他库。
- `run_test` 接收结构化 `command` 数组和存在的绝对 `cwd`，禁止 shell 字符串和命令参数携带测试密码。子进程仅获得 `DB_MCP_TEST_HOST/PORT/DATABASE/USER/PASSWORD/URL/JDBC_URL`、`SPRING_DATASOURCE_*` 与 `SPRING_DATASOURCE_DRUID_MASTER_*`，不获得镜像管理凭据；stdout/stderr 在流式读取时先脱敏，再写入任务日志和 64 KiB 有界响应缓冲，原始未脱敏输出不落盘。Windows 以挂起启动、Job Object 绑定后再恢复；绑定或恢复失败在负载执行前拒绝。POSIX 使用独立进程组；显式 `timeout_seconds` 超时返回 `timed_out`，省略时不限制测试进程时长；非零退出返回 `failed`，清理未确认返回 `cleanup_failed`。TLS 镜像连接暂不支持，不静默降级。该入口是受信任脚本运行器，不是 OS 级沙箱。需要固定库名的单镜像测试可在 MCP 环境配置 `DB_MCP_MIRROR_ALLOW_NAMED=1` 后使用 `mirror_name`，默认同源唯一镜像绑定不受影响。
- `db_task_result` 读取超过 10 秒的后台任务。默认只返回状态、最近进度、结果摘要和完整安全异常摘要，不整包返回结果或日志；`log_mode=tail` 配合 `offset`/`max_bytes` 续读，`log_mode=full` 读取完整脱敏日志，`line_start`/`line_end` 读取指定行；只有 `include_result=true` 才返回完整结果。日志默认位于服务目录 `run-logs/`，可用 `DB_MCP_RUN_LOG_DIR` 指定。
- 导出覆盖所选基础表，不包含视图、触发器、例程、事件；结果明确覆盖范围，依赖这些对象的任务不能宣称完整镜像验收。跨库外键/对象引用不自动改写，镜像权限不足即失败，不扩大权限。导出期间不得有并行 DDL；非事务表不承诺一致快照。
- 普通升级/修复 SQL 优先静态检查与必要只读核验，不自动建镜像。高危写入、删除、破坏性 DDL，或明确存在静态检查不能解决的执行疑点，才进行镜像验证。
- 幂等测试或受控应用测试执行首次、重复执行和必要失败路径后必须 `drop`，再 `status` 确认不存在才可交付。异常/失败也清理；清理失败必须报告未完成，禁止宣称完成。

## 剪枝缺口闭环

`db_test_dataset` 的数据准备采用可重复闭环，不把一次 `extend` 视为必然完成：

1. 使用来源库只读查询核对当前业务键、关联记录、配置和预期不变量。记录 SQL 条件、原始行以及源库预期。
2. 镜像与源库不一致时，将差异逐项归因到 `1=0`、过窄 `prune`、缺少依赖表或缺少基础配置。只有确认属于剪枝数据缺口时才进入增量补充。
3. 每轮把已经确认的缺口合并成一次 `action=extend`，追加到当前镜像和同一数据包。导出完成后立即使用 `verify_sql` 或同一测试入口做镜像隔离验证。加载失败或验证失败也保存 extension，并记录 `status`、`verificationStatus` 与失败原因。
4. 验证仍失败时，重新执行步骤 1，定位新的源库事实和残余缺口。确认仍属剪枝缺口后执行下一轮 `extend`；未归因的失败不得通过盲目补表处理。
5. 闭环在“镜像事实与源库一致并完成隔离验证”或“证据排除剪枝原因”时结束。结束后的测试仍必须统一 `drop` 镜像并核验不存在。

extension 使用连续编号，清单保存每轮的 `tables`、`prune`、`supplementTables`、导出引擎、验证 SQL 和执行状态。失败轮次不会被覆盖，下一轮使用新编号，便于区分剪枝缺口、初始数据范围与最终通过状态。

## 验证方式

运行 `python -m unittest discover -s tests -v` 验证离线逻辑及临时 SQLite 夹具。真实 MySQL 冒烟需要调用者自行配置目标和凭据，参数与执行边界见 [README](../README.md#安装与验证)。仓库不包含内部项目名称、业务数据、连接配置或现场执行日志。
