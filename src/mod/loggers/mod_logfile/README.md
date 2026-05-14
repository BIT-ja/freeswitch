# mod_logfile — FreeSWITCH 文件日志模块

## 概述

`mod_logfile` 是 FreeSWITCH 的日志后端模块，将日志写入本地文件。支持多配置文件（profile）、日志轮转、结构化前缀输出，以及按文件名/函数名过滤日志级别。

## 功能特性

- **多 Profile 支持**：可定义多个日志配置，各自独立的日志文件、过滤规则和前缀设置
- **日志轮转**：按文件大小自动轮转，支持两种模式：
  - **固定数量轮转**（`maximum-rotate`）：日志文件命名为 `freeswitch.log.1`、`freeswitch.log.2`...，循环覆盖，不包含时间戳
  - **时间戳轮转**（未设置 `maximum-rotate`）：轮转文件带时间戳后缀 `freeswitch.log.2025-04-27-10-30-00.1`
- **SIGHUP 处理**：收到 `SIGHUP` 信号时自动轮转（`rotate-on-hup=true`）或重新打开日志文件
- **结构化前缀**：在每行日志前附加结构化信息，便于日志采集和检索
  - **UUID 前缀**：在日志行首输出会话 UUID（`uuid=true`）
  - **日志标签**：输出通道日志标签（`log-tags=true`），配合 `set_log_tag` 应用使用
  - **通道变量前缀**：将指定通道变量以 `name:value` 格式输出到前缀（`channel-vars`）
- **通道变量缓存**：UUID 级别的通道变量前缀缓存，减少重复查表开销；会话销毁后自动标记过期并延迟清理

## 日志标签（Log Tags）

### set_log_tag 应用

`set_log_tag` 是 `mod_dptools` 提供的拨号计划应用，用于为通话通道设置日志标签。标签会随日志自动输出到 `mod_logfile`（需配置 `log-tags=true`）。

**语法**：
```
<action application="set_log_tag" data="tagname=tagvalue"/>
```

- `tagname=tagvalue` — 设置标签 `tagname` 的值为 `tagvalue`
- `tagname=` — 删除标签 `tagname`（值为空即删除）

**示例**：
```xml
<extension name="example">
  <condition field="destination_number" expression="^1000$">
    <!-- 设置日志标签 -->
    <action application="set_log_tag" data="callid=ABC-12345"/>
    <action application="set_log_tag" data="tenant=acme"/>
    <!-- 后续所有日志将自动携带这些标签 -->
    <action application="bridge" data="sofia/internal/1000@10.0.0.1"/>
  </condition>
</extension>
```

### log-tags 与 channel-vars 的区别

| 特性 | `log-tags`（日志标签） | `channel-vars`（通道变量前缀） |
|------|------------------------|-------------------------------|
| 数据来源 | `switch_channel_set_log_tag()` 设置的标签 | 通道变量（`switch_channel_get_variable()`） |
| 设置方式 | 拨号计划中 `set_log_tag` 应用 | 通过 SIP 头、`set` 应用等设置 |
| 配置方式 | `log-tags=true` | `channel-vars=label=var,...` |
| 缓存机制 | 无（标签直接随日志节点传递） | 有（UUID 级别前缀缓存） |
| 适用场景 | 主动标注业务字段（租户ID、呼叫ID等） | 被动读取已有通道变量 |

**推荐**：优先使用 `set_log_tag` + `log-tags=true`，无需缓存、无查表开销，语义更清晰。

## 配置文件

配置文件路径：`autoload_configs/logfile.conf.xml`

### 全局参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `rotate-on-hup` | 收到 SIGHUP 时是否轮转日志。`true` = 轮转，`false` = 关闭并重新打开 | `true` |

### Profile 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `logfile` | 日志文件路径 | `$LOG_DIR/freeswitch.log` |
| `rollover` | 日志文件大小达到此字节数时触发轮转，`0` 表示永不轮转 | `0` |
| `maximum-rotate` | 保留的最大轮转文件数量。启用后文件名不含时间戳；设为 `0` 等同于 `4096` | 未启用（使用时间戳模式） |
| `uuid` | 是否在日志行首输出会话 UUID | `true` |
| `log-tags` | 是否输出通道日志标签 | `false` |
| `channel-vars` | 通道变量映射，格式见下方说明 | 无 |

### channel-vars 映射格式

`channel-vars` 参数将通道变量以 `label:值` 形式附加到日志前缀。支持两种写法：

- `label=channel_variable` — 用 `label` 作为前缀中的显示名，读取通道变量 `channel_variable` 的值
- `channel_variable` — 简写形式，label 与变量名相同

多个映射以逗号分隔。

**示例**：
```xml
<param name="channel-vars" value="callid=sip_call_id,instanceID=sip_h_X-callinstanceID"/>
```
前缀输出效果：`callid:ABC123 instanceID:inst456 `

### Mappings（日志级别映射）

通过 `<map>` 标签按源文件名或函数名过滤日志级别：

```xml
<mappings>
  <map name="all" value="debug,info,notice,warning,err,crit,alert"/>
  <map name="sofia.c" value="warning,err,crit"/>
  <map name="my_func" value="debug,info"/>
</mappings>
```

匹配优先级（从高到低）：
1. `all` — 全局默认级别
2. `文件名:函数名` — 精确匹配
3. `函数名` — 按函数名匹配
4. `文件名` — 按源文件名匹配

## 日志输出格式

开启结构化前缀后，日志行格式为：

```
[时间] [级别] UUID logtag:值 label:值 ... 原始日志内容
```

前缀部分为纯文本 token，不使用方括号包裹。例如：

```
2025-04-27 10:30:00.000000 [INFO] a1b2c3d4 callid:ABC123 instanceID:inst456 Channel answered
```

### Token 安全处理

前缀中的 token 经过安全过滤：
- 回车、换行、制表符、`[`、`]` 替换为 `_`
- 其他控制字符（< 32 或 127）替换为 `?`
- 名称最大 128 字节，值最大 512 字节

## 通道变量缓存机制

为避免每条日志都查询通道变量带来的性能开销，模块实现了 UUID 级别的前缀缓存：

| 参数 | 值 | 说明 |
|------|-----|------|
| `LOG_TAG_VAR_CACHE_MAX_ENTRIES` | 200,000 | 缓存最大条目数 |
| `LOG_TAG_VAR_CACHE_REFRESH_USEC` | 250,000 (250ms) | 未完成的缓存条目刷新间隔 |
| `LOG_TAG_VAR_CACHE_STALE_USEC` | 10,000,000 (10s) | 过期条目延迟清理时间 |
| `LOG_TAG_VAR_CACHE_SWEEP_INTERVAL` | 1024 次日志写入 | 触发缓存清理的写入次数 |

缓存生命周期：
1. **创建**：首次遇到某 UUID 的日志时，查询通道变量并缓存前缀
2. **更新**：当缓存标记为不完整（未找到全部变量）或刷新时间到期时，重新查询并更新
3. **标记过期**：收到 `CHANNEL_DESTROY` 事件时，将对应 UUID 的缓存标记为 stale
4. **延迟清理**：每 1024 次日志写入触发一次扫描，清理过期超过 10 秒的缓存条目
5. **强制清理**：Profile 卸载时强制清除全部缓存

## 示例配置

### 基础配置

```xml
<configuration name="logfile.conf" description="File Logging">
  <settings>
    <param name="rotate-on-hup" value="true"/>
  </settings>
  <profiles>
    <profile name="default">
      <settings>
        <param name="rollover" value="10485760"/>
        <param name="maximum-rotate" value="32"/>
        <param name="uuid" value="true"/>
      </settings>
      <mappings>
        <map name="all" value="debug,info,notice,warning,err,crit,alert"/>
      </mappings>
    </profile>
  </profiles>
</configuration>
```

### 启用结构化前缀的配置

```xml
<configuration name="logfile.conf" description="File Logging">
  <settings>
    <param name="rotate-on-hup" value="true"/>
  </settings>
  <profiles>
    <profile name="default">
      <settings>
        <param name="logfile" value="/var/log/freeswitch/freeswitch.log"/>
        <param name="rollover" value="1048576000"/>
        <param name="maximum-rotate" value="32"/>
        <param name="uuid" value="true"/>
        <param name="log-tags" value="true"/>
      </settings>
      <mappings>
        <map name="all" value="console,debug,info,notice,warning,err,crit,alert"/>
      </mappings>
    </profile>
  </profiles>
</configuration>
```

拨号计划中配合 `set_log_tag` 使用：
```xml
<action application="set_log_tag" data="callid=ABC-12345"/>
<action application="set_log_tag" data="tenant=acme"/>
```

日志输出效果：
```
2025-04-27 10:30:00.000000 [INFO] a1b2c3d4 callid:ABC-12345 tenant:acme Channel answered
```

### 多 Profile 配置

```xml
<configuration name="logfile.conf" description="File Logging">
  <settings>
    <param name="rotate-on-hup" value="true"/>
  </settings>
  <profiles>
    <!-- 主日志：记录全部级别 -->
    <profile name="default">
      <settings>
        <param name="rollover" value="104857600"/>
        <param name="maximum-rotate" value="32"/>
        <param name="uuid" value="true"/>
        <param name="log-tags" value="true"/>
      </settings>
      <mappings>
        <map name="all" value="warning,err,crit,alert"/>
      </mappings>
    </profile>
    <!-- 调试日志：仅记录 SIP 相关的 DEBUG 级别 -->
    <profile name="sip_debug">
      <settings>
        <param name="logfile" value="/var/log/freeswitch/sip_debug.log"/>
        <param name="rollover" value="52428800"/>
        <param name="maximum-rotate" value="10"/>
        <param name="uuid" value="true"/>
      </settings>
      <mappings>
        <map name="sofia.c" value="debug"/>
        <map name="sofia_glue.c" value="debug"/>
      </mappings>
    </profile>
  </profiles>
</configuration>
```

## Profile 加载时的日志输出

Profile 加载成功后会打印一条 NOTICE 级别的日志，包含全部生效配置：

```
[NOTICE] mod_logfile profile=default logfile=/var/log/freeswitch/freeswitch.log rollover=1048576000 maximum-rotate=32 uuid=true log-tags=true channel-vars=callid=sip_call_id mappings=1
```

其中 `channel-vars` 显示格式为：`label=var`（label 与变量不同时）或 `var`（简写形式），多项以逗号分隔。

## 缓存调试

缓存相关操作会在 `DEBUG` 级别输出日志，包括：
- 缓存创建（`cache create`）
- 缓存更新（`cache update`）
- 缓存清理（`cache clear`），含清理原因（`null-entry`、`force`、`stale-expired`）
- 清理汇总（`cache clear summary`），含删除数量和剩余数量
