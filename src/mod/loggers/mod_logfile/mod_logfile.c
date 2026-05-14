/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 *
 * Version: MPL 1.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 *
 * The Initial Developer of the Original Code is
 * Anthony Minessale II <anthm@freeswitch.org>
 * Portions created by the Initial Developer are Copyright (C)
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 *
 * Anthony LaMantia <anthony@petabit.net>
 * Michael Jerris <mike@jerris.com>
 *
 *
 * mod_logfile.c -- FreeSWITCH 文件日志模块
 *
 * 将日志写入本地文件，支持多 Profile、日志轮转、结构化前缀输出
 * （UUID、日志标签、通道变量）以及 UUID 级别的前缀缓存。
 *
 */

#include <switch.h>

SWITCH_MODULE_LOAD_FUNCTION(mod_logfile_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_logfile_shutdown);
SWITCH_MODULE_DEFINITION(mod_logfile, mod_logfile_load, mod_logfile_shutdown, NULL);

/* ---- 常量定义 ---- */

#define DEFAULT_LIMIT	 0xA00000	/* 默认日志大小限制，约 10 MB */
#define WARM_FUZZY_OFFSET 256		/* 文件名缓冲区额外预留空间 */
#define MAX_ROT 4096			/* 最大轮转文件数上限 */

/* Token（前缀键值对）长度限制 */
#define LOG_TOKEN_NAME_MAX 128		/* 键名最大字节数 */
#define LOG_TOKEN_VALUE_MAX 512		/* 值最大字节数 */

/* 通道变量前缀缓存相关常量 */
#define LOG_TAG_VAR_CACHE_SWEEP_INTERVAL 1024	/* 每写入多少条日志触发一次缓存扫描清理 */
#define LOG_TAG_VAR_CACHE_STALE_USEC (10 * 1000000)	/* 过期条目延迟清理时间：10 秒 */
#define LOG_TAG_VAR_CACHE_REFRESH_USEC (250 * 1000)	/* 未完成缓存的刷新间隔：250 毫秒 */
#define LOG_TAG_VAR_CACHE_MAX_ENTRIES 200000		/* 缓存最大条目数 */

/* ---- 全局状态 ---- */

static switch_memory_pool_t *module_pool = NULL;	/* 模块内存池 */
static switch_hash_t *profile_hash = NULL;		/* Profile 哈希表，以 profile 名称为键 */

/* 全局配置与事件绑定句柄 */
static struct {
	int rotate;				/* 收到 SIGHUP 时是否轮转日志 */
	switch_mutex_t *mutex;			/* 全局文件操作互斥锁 */
	switch_event_node_t *node;		/* SIGHUP 事件绑定句柄 */
	switch_event_node_t *destroy_node;	/* CHANNEL_DESTROY 事件绑定句柄 */
} globals;

/* ---- 通道变量前缀缓存条目 ---- */
typedef struct mod_logfile_tag_var_cache_entry_s {
	char *prefix;				/* 缓存的前缀字符串 */
	switch_bool_t complete;			/* 前缀是否包含全部已配置的通道变量 */
	switch_bool_t stale;			/* 是否已标记为过期（通道已销毁） */
	switch_time_t refresh_after;		/* 下次刷新时间（微秒），complete=true 时为 0 */
	switch_time_t stale_at;			/* 标记为过期的时间（微秒），用于延迟清理 */
} mod_logfile_tag_var_cache_entry_t;

/* ---- 日志 Profile 结构体 ---- */
struct logfile_profile {
	char *name;				/* Profile 名称 */
	switch_size_t log_size;			/* 当前日志文件大小（字节），用于判断是否需要轮转 */
	switch_size_t roll_size;		/* 触发轮转的文件大小阈值（字节），0 表示永不轮转 */
	switch_size_t max_rot;			/* 最大轮转文件保留数量，0 表示使用时间戳命名模式 */
	char *logfile;				/* 日志文件路径 */
	switch_file_t *log_afd;		/* 当前打开的日志文件句柄 */
	switch_hash_t *log_hash;		/* 日志级别映射哈希表（文件名/函数名 → 级别掩码） */
	uint32_t all_level;			/* "all" 映射对应的日志级别掩码 */
	uint32_t suffix;			/* 固定数量轮转模式下，当前最大文件编号后缀 */
	switch_bool_t log_uuid;		/* 是否在日志前缀中输出会话 UUID */
	switch_bool_t log_tags;		/* 是否在日志前缀中输出通道日志标签（配合 set_log_tag 应用使用） */
	switch_event_t *tag_var_map;		/* 通道变量映射表（label → channel_variable_name） */
	switch_hash_t *tag_var_cache;		/* UUID 级别的前缀缓存哈希表 */
	switch_mutex_t *tag_var_cache_mutex;	/* 前缀缓存互斥锁 */
	switch_size_t tag_var_cache_size;	/* 缓存条目数量 */
	switch_size_t tag_var_count;		/* 已配置的通道变量映射数量 */
	uint32_t tag_var_cache_tick;		/* 日志写入计数器，达到 SWEEP_INTERVAL 时触发缓存扫描 */
};

typedef struct logfile_profile logfile_profile_t;

static switch_status_t load_profile(switch_xml_t xml);

#if 0
static void del_mapping(char *var, logfile_profile_t *profile)
{
	switch_core_hash_insert(profile->log_hash, var, NULL);
}
#endif

/* 添加日志级别映射。当 name 为 "all" 时设置全局级别掩码，否则按文件名/函数名存入哈希表 */
static void add_mapping(logfile_profile_t *profile, char *var, char *val)
{
	if (!strcasecmp(var, "all")) {
		profile->all_level |= (uint32_t) switch_log_str2mask(val);
		return;
	}

	switch_core_hash_insert(profile->log_hash, var, (void *) (intptr_t) switch_log_str2mask(val));
}

static switch_status_t mod_logfile_rotate(logfile_profile_t *profile);

/* 打开日志文件。check=true 时检查文件大小是否达到轮转阈值 */
static switch_status_t mod_logfile_openlogfile(logfile_profile_t *profile, switch_bool_t check)
{
	unsigned int flags = 0;
	switch_file_t *afd;
	switch_status_t stat;

	flags |= SWITCH_FOPEN_CREATE;
	flags |= SWITCH_FOPEN_READ;
	flags |= SWITCH_FOPEN_WRITE;
	flags |= SWITCH_FOPEN_APPEND;

	stat = switch_file_open(&afd, profile->logfile, flags, SWITCH_FPROT_OS_DEFAULT, module_pool);
	if (stat != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "logfile %s open error, status=%d\n", profile->logfile, stat);

		return SWITCH_STATUS_FALSE;
	}

	profile->log_afd = afd;

	profile->log_size = switch_file_get_size(profile->log_afd);

	if (check && profile->roll_size && profile->log_size >= profile->roll_size) {
		mod_logfile_rotate(profile);
	}

	return SWITCH_STATUS_SUCCESS;
}

/* 日志文件轮转。
 * 固定数量模式（max_rot > 0）：freeswitch.log → .1 → .2 → ... → .N，循环覆盖，文件名不含时间戳
 * 时间戳模式（max_rot == 0）：重命名为 freeswitch.log.YYYY-MM-DD-HH-MM-SS.N，递增序号防同秒冲突 */
static switch_status_t mod_logfile_rotate(logfile_profile_t *profile)
{
	unsigned int i = 0;
	char *filename = NULL;
	switch_status_t stat = 0;
	int64_t offset = 0;
	switch_memory_pool_t *pool = NULL;
	switch_time_exp_t tm;
	char date[80] = "";
	switch_size_t retsize;
	switch_status_t status = SWITCH_STATUS_SUCCESS;

	switch_mutex_lock(globals.mutex);

	switch_time_exp_lt(&tm, switch_micro_time_now());
	switch_strftime_nocheck(date, &retsize, sizeof(date), "%Y-%m-%d-%H-%M-%S", &tm);

	profile->log_size = 0;

	stat = switch_file_seek(profile->log_afd, SWITCH_SEEK_SET, &offset);

	if (stat != SWITCH_STATUS_SUCCESS) {
		status = SWITCH_STATUS_FALSE;
		goto end;
	}

	switch_core_new_memory_pool(&pool);
	filename = switch_core_alloc(pool, strlen(profile->logfile) + WARM_FUZZY_OFFSET);

	if (profile->max_rot) {
		char *from_filename = NULL;
		char *to_filename = NULL;

		from_filename = switch_core_alloc(pool, strlen(profile->logfile) + WARM_FUZZY_OFFSET);
		to_filename = switch_core_alloc(pool, strlen(profile->logfile) + WARM_FUZZY_OFFSET);

		/* 固定数量轮转：从高编号向低编号依次重命名，腾出位置
		 * 例如 max_rot=3, suffix=3 时：.3 删除，.2→.3，.1→.2 */
		for (i=profile->suffix; i>1; i--) {
			sprintf((char *) to_filename, "%s.%i", profile->logfile, i);
			sprintf((char *) from_filename, "%s.%i", profile->logfile, i-1);

			if (switch_file_exists(to_filename, pool) == SWITCH_STATUS_SUCCESS) {
				if ((status = switch_file_remove(to_filename, pool)) != SWITCH_STATUS_SUCCESS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error removing log %s\n",to_filename);
					goto end;
				}
			}

			if ((status = switch_file_rename(from_filename, to_filename, pool)) != SWITCH_STATUS_SUCCESS) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error renaming log from %s to %s [%s]\n",
								  from_filename, to_filename, strerror(errno));
				if (errno != ENOENT) {
					goto end;
				}
			}
		}

		/* 将当前日志文件重命名为 .1（先删除已存在的 .1 文件） */
		sprintf((char *) to_filename, "%s.%i", profile->logfile, i);

		if (switch_file_exists(to_filename, pool) == SWITCH_STATUS_SUCCESS) {
			if ((status = switch_file_remove(to_filename, pool)) != SWITCH_STATUS_SUCCESS) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error removing log %s [%s]\n", to_filename, strerror(errno));
				goto end;
			}
		}

		switch_file_close(profile->log_afd);
		if ((status = switch_file_rename(profile->logfile, to_filename, pool)) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error renaming log from %s to %s [%s]\n", profile->logfile, to_filename, strerror(errno));
			goto end;
		}

		if ((status = mod_logfile_openlogfile(profile, SWITCH_FALSE)) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error reopening log %s\n", profile->logfile);
		}
		if (profile->suffix < profile->max_rot) {
			profile->suffix++;
		}

		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "New log started: %s\n", profile->logfile);

		goto end;
	}

	/* 时间戳轮转模式：重命名为 logfile.YYYY-MM-DD-HH-MM-SS.N
	 * 递增序号确保同一秒内多次轮转不会覆盖 */
	for (i = 1; i < MAX_ROT; i++) {
		sprintf((char *) filename, "%s.%s.%i", profile->logfile, date, i);
		if (switch_file_exists(filename, pool) == SWITCH_STATUS_SUCCESS) {
			continue;
		}

		switch_file_close(profile->log_afd);
		switch_file_rename(profile->logfile, filename, pool);
		if ((status = mod_logfile_openlogfile(profile, SWITCH_FALSE)) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error Rotating Log!\n");
			goto end;
		}
		break;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "New log started.\n");

  end:

	if (pool) {
		switch_core_destroy_memory_pool(&pool);
	}

	switch_mutex_unlock(globals.mutex);

	return status;
}

/* 将日志数据写入文件。写入失败时尝试重新打开日志文件并重试 */
static switch_status_t mod_logfile_raw_write(logfile_profile_t *profile, char *log_data)
{
	switch_size_t len;
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	len = strlen(log_data);

	if (len <= 0 || !profile->log_afd) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(globals.mutex);

	if (switch_file_write(profile->log_afd, log_data, &len) != SWITCH_STATUS_SUCCESS) {
		switch_file_close(profile->log_afd);
		if ((status = mod_logfile_openlogfile(profile, SWITCH_TRUE)) == SWITCH_STATUS_SUCCESS) {
			len = strlen(log_data);
			switch_file_write(profile->log_afd, log_data, &len);
		}
	}

	switch_mutex_unlock(globals.mutex);

	if (status == SWITCH_STATUS_SUCCESS) {
		profile->log_size += len;

		if (profile->roll_size && profile->log_size >= profile->roll_size) {
			mod_logfile_rotate(profile);
		}
	}

	return status;
}

/* Token 安全过滤：将回车/换行/制表符/方括号替换为 '_'，其他控制字符替换为 '?' */
static void mod_logfile_sanitize_token(char *dst, size_t dst_len, const char *src)
{
	size_t i = 0;

	switch_assert(dst != NULL);
	switch_assert(dst_len > 0);

	if (zstr(src)) {
		*dst = '\0';
		return;
	}

	for (; *src && i < dst_len - 1; src++) {
		unsigned char c = (unsigned char) *src;

		if (c == '\r' || c == '\n' || c == '\t' || c == '[' || c == ']') {
			dst[i++] = '_';
		} else if (c < 32 || c == 127) {
			dst[i++] = '?';
		} else {
			dst[i++] = (char) c;
		}
	}

	dst[i] = '\0';
}

/* 将键值对以 "name:value " 格式追加到流中，自动进行安全过滤 */
static void mod_logfile_append_kv_prefix(switch_stream_handle_t *stream, const char *name, const char *value)
{
	char safe_name[LOG_TOKEN_NAME_MAX + 1] = "";
	char safe_value[LOG_TOKEN_VALUE_MAX + 1] = "";

	switch_assert(stream != NULL);

	mod_logfile_sanitize_token(safe_name, sizeof(safe_name), name);
	mod_logfile_sanitize_token(safe_value, sizeof(safe_value), value);

	if (!zstr(safe_name) && !zstr(safe_value)) {
		stream->write_function(stream, "%s:%s ", safe_name, safe_value);
	}
}

/* 将通道变量映射格式化为字符串，用于 Profile 加载日志输出。
 * label 与变量名相同时输出 "var"，不同时输出 "label=var"，多项以逗号分隔 */
static char *mod_logfile_format_channel_vars(logfile_profile_t *profile)
{
	switch_stream_handle_t stream = { 0 };
	switch_event_header_t *hp;
	int first = 1;

	if (!profile || !profile->tag_var_map || !profile->tag_var_map->headers) {
		return NULL;
	}

	SWITCH_STANDARD_STREAM(stream);

	for (hp = profile->tag_var_map->headers; hp; hp = hp->next) {
		if (zstr(hp->name) || zstr(hp->value)) {
			continue;
		}

		if (!first) {
			stream.write_function(&stream, ",");
		}

		if (!strcmp(hp->name, hp->value)) {
			stream.write_function(&stream, "%s", hp->name);
		} else {
			stream.write_function(&stream, "%s=%s", hp->name, hp->value);
		}
		first = 0;
	}

	if (first && stream.data) {
		switch_safe_free(stream.data);
	}

	return (char *) stream.data;
}

/* 输出 Profile 加载成功后的配置摘要日志（NOTICE 级别） */
static void mod_logfile_log_profile_config(logfile_profile_t *profile)
{
	switch_hash_index_t *hi;
	size_t mapping_count = 0;
	char *channel_vars = NULL;

	if (!profile) {
		return;
	}

	for (hi = switch_core_hash_first(profile->log_hash); hi; hi = switch_core_hash_next(&hi)) {
		mapping_count++;
	}

	channel_vars = mod_logfile_format_channel_vars(profile);

	switch_log_printf(
		SWITCH_CHANNEL_LOG,
		SWITCH_LOG_NOTICE,
		"mod_logfile profile=%s logfile=%s rollover=%" SWITCH_SIZE_T_FMT
		" maximum-rotate=%" SWITCH_SIZE_T_FMT " uuid=%s log-tags=%s channel-vars=%s mappings=%" SWITCH_SIZE_T_FMT "\n",
		switch_str_nil(profile->name),
		switch_str_nil(profile->logfile),
		profile->roll_size,
		profile->max_rot,
		profile->log_uuid ? "true" : "false",
		profile->log_tags ? "true" : "false",
		channel_vars ? channel_vars : "(none)",
		mapping_count
	);

	switch_safe_free(channel_vars);
}

/* 解析 channel-vars 配置字符串，构建 label → channel_variable_name 映射表。
 * 支持格式：label=var 或 var（简写，label 与 var 相同），多项以逗号分隔 */
static void mod_logfile_add_tag_vars(logfile_profile_t *profile, const char *data)
{
	char *argv[128] = { 0 };
	char *item;
	char *dup = NULL;
	int argc, i;

	if (!profile || zstr(data)) {
		return;
	}

	dup = strdup(data);
	switch_assert(dup);
	argc = switch_separate_string(dup, ',', argv, (sizeof(argv) / sizeof(argv[0])));

	for (i = 0; i < argc; i++) {
		char *label = NULL;
		char *var = NULL;

		item = switch_strip_spaces(argv[i], SWITCH_FALSE);
		if (zstr(item)) {
			continue;
		}

		if ((var = strchr(item, '='))) {
			*var++ = '\0';
			label = switch_strip_spaces(item, SWITCH_FALSE);
			var = switch_strip_spaces(var, SWITCH_FALSE);
		} else {
			label = var = item;
		}

		if (!zstr(label) && !zstr(var)) {
			if (!profile->tag_var_map) {
				switch_event_create_plain(&profile->tag_var_map, SWITCH_EVENT_CHANNEL_DATA);
			}
			switch_event_add_header_string(profile->tag_var_map, SWITCH_STACK_BOTTOM, label, var);
			profile->tag_var_count++;
		}
	}

	switch_safe_free(dup);
}

/* 缓存条目销毁回调，释放前缀字符串和条目本身 */
static void mod_logfile_tag_var_cache_entry_destroy(void *ptr)
{
	mod_logfile_tag_var_cache_entry_t *entry = (mod_logfile_tag_var_cache_entry_t *) ptr;

	if (!entry) {
		return;
	}

	switch_safe_free(entry->prefix);
	switch_safe_free(entry);
}

/* 缓存垃圾回收上下文，传递给哈希表遍历删除回调 */
typedef struct mod_logfile_tag_var_gc_ctx_s {
	switch_time_t now;			/* 当前时间（微秒） */
	switch_size_t deleted;			/* 已删除的条目计数 */
	switch_bool_t force;			/* 是否强制删除全部条目（Profile 卸载时） */
	logfile_profile_t *profile;		/* 所属 Profile */
} mod_logfile_tag_var_gc_ctx_t;

/* 计算缓存中的实际条目数量（遍历哈希表） */
static switch_size_t mod_logfile_tag_var_cache_count(logfile_profile_t *profile)
{
	switch_hash_index_t *hi;
	switch_size_t count = 0;

	for (hi = switch_core_hash_first(profile->tag_var_cache); hi; hi = switch_core_hash_next(&hi)) {
		count++;
	}

	return count;
}

/* 缓存条目删除判断回调。
 * 返回 SWITCH_TRUE 表示删除该条目，删除条件：
 * 1. 条目为空（null-entry）
 * 2. force=true（强制清理）
 * 3. 条目已过期且过期时间超过 STALE_USEC（10 秒） */
static switch_bool_t mod_logfile_tag_var_cache_delete_cb(const void *key, const void *val, void *pData)
{
	mod_logfile_tag_var_gc_ctx_t *ctx = (mod_logfile_tag_var_gc_ctx_t *) pData;
	mod_logfile_tag_var_cache_entry_t *entry = (mod_logfile_tag_var_cache_entry_t *) val;

	(void) key;

	if (!ctx) {
		return SWITCH_FALSE;
	}

	if (!entry) {
		ctx->deleted++;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache clear profile=%s uuid=%s reason=null-entry\n",
						  ctx->profile ? switch_str_nil(ctx->profile->name) : "(unknown)",
						  switch_str_nil((const char *) key));
		return SWITCH_TRUE;
	}

	if (ctx->force) {
		ctx->deleted++;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache clear profile=%s uuid=%s reason=force\n",
						  ctx->profile ? switch_str_nil(ctx->profile->name) : "(unknown)",
						  switch_str_nil((const char *) key));
		return SWITCH_TRUE;
	}

	if (entry->stale && entry->stale_at && ((ctx->now - entry->stale_at) >= LOG_TAG_VAR_CACHE_STALE_USEC)) {
		ctx->deleted++;
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache clear profile=%s uuid=%s reason=stale-expired\n",
						  ctx->profile ? switch_str_nil(ctx->profile->name) : "(unknown)",
						  switch_str_nil((const char *) key));
		return SWITCH_TRUE;
	}

	return SWITCH_FALSE;
}

/* 扫描并清理过期的缓存条目。force=true 时清除全部条目（用于 Profile 卸载） */
static void mod_logfile_tag_var_cache_sweep(logfile_profile_t *profile, switch_time_t now, switch_bool_t force)
{
	mod_logfile_tag_var_gc_ctx_t ctx = { 0 };

	if (!profile || !profile->tag_var_cache || !profile->tag_var_cache_mutex) {
		return;
	}

	ctx.now = now;
	ctx.force = force;
	ctx.profile = profile;

	switch_mutex_lock(profile->tag_var_cache_mutex);
	switch_core_hash_delete_multi(profile->tag_var_cache, mod_logfile_tag_var_cache_delete_cb, &ctx);
	if (ctx.deleted) {
		profile->tag_var_cache_size = mod_logfile_tag_var_cache_count(profile);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache clear summary profile=%s deleted=%" SWITCH_SIZE_T_FMT " remaining=%" SWITCH_SIZE_T_FMT " force=%s\n",
						  switch_str_nil(profile->name),
						  ctx.deleted,
						  profile->tag_var_cache_size,
						  force ? "true" : "false");
	}
	switch_mutex_unlock(profile->tag_var_cache_mutex);
}

/* 查找或创建指定 UUID 的缓存条目（需在 tag_var_cache_mutex 已加锁状态下调用）。
 * 缓存条目数达到上限时返回 NULL */
static mod_logfile_tag_var_cache_entry_t *mod_logfile_tag_var_cache_get_or_create_locked(logfile_profile_t *profile, const char *uuid)
{
	mod_logfile_tag_var_cache_entry_t *entry;

	switch_assert(profile != NULL);
	switch_assert(profile->tag_var_cache != NULL);

	entry = switch_core_hash_find(profile->tag_var_cache, uuid);
	if (entry) {
		return entry;
	}

	if (profile->tag_var_cache_size >= LOG_TAG_VAR_CACHE_MAX_ENTRIES) {
		return NULL;
	}

	entry = calloc(1, sizeof(*entry));
	if (!entry) {
		return NULL;
	}

	if (switch_core_hash_insert_destructor(profile->tag_var_cache, uuid, entry, mod_logfile_tag_var_cache_entry_destroy) == SWITCH_STATUS_SUCCESS) {
		profile->tag_var_cache_size++;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache create profile=%s uuid=%s cache-size=%" SWITCH_SIZE_T_FMT " prefix-bytes=0 prefix=(empty)\n",
						  switch_str_nil(profile->name),
						  switch_str_nil(uuid),
						  profile->tag_var_cache_size);
		return entry;
	}

	switch_safe_free(entry);
	return NULL;
}

/* 更新指定 UUID 的缓存条目：设置前缀、完成状态和刷新时间。
 * complete=true 时表示已找到全部通道变量，不再定期刷新 */
static void mod_logfile_tag_var_cache_update(logfile_profile_t *profile, const char *uuid, const char *prefix, switch_bool_t complete, switch_time_t now)
{
	mod_logfile_tag_var_cache_entry_t *entry;
	switch_size_t prefix_len = 0;

	if (!profile || !profile->tag_var_cache || !profile->tag_var_cache_mutex || zstr(uuid)) {
		return;
	}

	switch_mutex_lock(profile->tag_var_cache_mutex);
	entry = mod_logfile_tag_var_cache_get_or_create_locked(profile, uuid);
	if (entry) {
		if (!zstr(prefix) && (zstr(entry->prefix) || strcmp(entry->prefix, prefix))) {
			switch_safe_free(entry->prefix);
			entry->prefix = strdup(prefix);
		}
		entry->complete = complete;
		entry->stale = SWITCH_FALSE;
		entry->stale_at = 0;
		entry->refresh_after = complete ? 0 : (now + LOG_TAG_VAR_CACHE_REFRESH_USEC);
		prefix_len = (switch_size_t) (zstr(entry->prefix) ? 0 : strlen(entry->prefix));
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
						  "mod_logfile cache update profile=%s uuid=%s cache-size=%" SWITCH_SIZE_T_FMT
						  " prefix-bytes=%" SWITCH_SIZE_T_FMT " complete=%s prefix=%s\n",
						  switch_str_nil(profile->name),
						  switch_str_nil(uuid),
						  profile->tag_var_cache_size,
						  prefix_len,
						  entry->complete ? "true" : "false",
						  switch_str_nil(entry->prefix));
	}
	switch_mutex_unlock(profile->tag_var_cache_mutex);
}

/* 缓存未命中记录：从会话查询通道变量失败时，设置定期刷新时间以便后续重试 */
static void mod_logfile_tag_var_cache_note_miss(logfile_profile_t *profile, const char *uuid, switch_time_t now)
{
	mod_logfile_tag_var_cache_entry_t *entry;

	if (!profile || !profile->tag_var_cache || !profile->tag_var_cache_mutex || zstr(uuid)) {
		return;
	}

	switch_mutex_lock(profile->tag_var_cache_mutex);
	entry = mod_logfile_tag_var_cache_get_or_create_locked(profile, uuid);
	if (entry && !entry->complete && !entry->stale) {
		entry->refresh_after = now + LOG_TAG_VAR_CACHE_REFRESH_USEC;
	}
	switch_mutex_unlock(profile->tag_var_cache_mutex);
}

/* 将指定 UUID 的缓存条目标记为过期（由 CHANNEL_DESTROY 事件触发），
 * 过期条目不会立即删除，而是在下次缓存扫描时延迟清理 */
static void mod_logfile_tag_var_cache_mark_stale(logfile_profile_t *profile, const char *uuid, switch_time_t now)
{
	mod_logfile_tag_var_cache_entry_t *entry;

	if (!profile || !profile->tag_var_cache || !profile->tag_var_cache_mutex || zstr(uuid)) {
		return;
	}

	switch_mutex_lock(profile->tag_var_cache_mutex);
	entry = switch_core_hash_find(profile->tag_var_cache, uuid);
	if (entry) {
		entry->stale = SWITCH_TRUE;
		entry->stale_at = now;
		entry->refresh_after = 0;
	}
	switch_mutex_unlock(profile->tag_var_cache_mutex);
}

/* 从会话中查询已配置的通道变量，构建前缀字符串。
 * 返回 "label:value label:value " 格式的字符串，并通过 complete 参数
 * 指示是否已找到全部已配置的通道变量（found >= tag_var_count） */
static char *mod_logfile_build_channel_var_prefix_from_session(logfile_profile_t *profile, const char *uuid, switch_bool_t *complete)
{
	switch_stream_handle_t stream = { 0 };
	switch_event_header_t *hp;
	switch_core_session_t *session = NULL;
	switch_channel_t *channel = NULL;
	switch_size_t found = 0;

	if (complete) {
		*complete = SWITCH_FALSE;
	}

	if (!profile || !profile->tag_var_map || !profile->tag_var_map->headers || zstr(uuid)) {
		return NULL;
	}

	if (!(session = switch_core_session_locate(uuid))) {
		return NULL;
	}

	SWITCH_STANDARD_STREAM(stream);
	channel = switch_core_session_get_channel(session);

	for (hp = profile->tag_var_map->headers; hp; hp = hp->next) {
		if (!zstr(hp->name) && !zstr(hp->value)) {
			const char *val = switch_channel_get_variable(channel, hp->value);
			if (!zstr(val)) {
				mod_logfile_append_kv_prefix(&stream, hp->name, val);
				found++;
			}
		}
	}

	switch_core_session_rwunlock(session);

	if (!found) {
		switch_safe_free(stream.data);
		return NULL;
	}

	if (complete && profile->tag_var_count && found >= profile->tag_var_count) {
		*complete = SWITCH_TRUE;
	}

	return (char *) stream.data;
}

/* 构建日志行的结构化前缀，按顺序拼接：
 * 1. UUID 前缀（log_uuid=true 时）
 * 2. 通道日志标签（log_tags=true 时，配合 set_log_tag 应用使用）
 * 3. 通道变量前缀（channel-vars 配置存在时，带 UUID 级别缓存）
 *
 * 缓存刷新策略：
 * - 缓存不存在或未完成且刷新时间到期 → 从会话重新查询
 * - 查询成功 → 更新缓存并使用新前缀
 * - 查询失败（会话已不存在） → 记录 miss，使用旧缓存
 * - 每写入 SWEEP_INTERVAL 条日志触发一次过期条目扫描 */
static char *mod_logfile_build_prefix(logfile_profile_t *profile, const switch_log_node_t *node)
{
	switch_stream_handle_t stream = { 0 };
	switch_event_header_t *hp;

	SWITCH_STANDARD_STREAM(stream);

	if (profile->log_uuid && !zstr(node->userdata)) {
		char safe_uuid[LOG_TOKEN_VALUE_MAX + 1] = "";
		mod_logfile_sanitize_token(safe_uuid, sizeof(safe_uuid), node->userdata);
		stream.write_function(&stream, "%s ", safe_uuid);
	}

	if (profile->log_tags && node->tags) {
		for (hp = node->tags->headers; hp; hp = hp->next) {
			if (!zstr(hp->name) && !zstr(hp->value)) {
				mod_logfile_append_kv_prefix(&stream, hp->name, hp->value);
			}
		}
	}

	if (profile->tag_var_map && profile->tag_var_map->headers && !zstr(node->userdata)) {
		switch_bool_t need_refresh = SWITCH_FALSE;
		switch_time_t now = switch_micro_time_now();
		mod_logfile_tag_var_cache_entry_t *entry = NULL;
		char *fresh_prefix = NULL;
		switch_bool_t fresh_complete = SWITCH_FALSE;

		switch_mutex_lock(profile->tag_var_cache_mutex);
		entry = switch_core_hash_find(profile->tag_var_cache, node->userdata);
		if (!entry) {
			need_refresh = SWITCH_TRUE;
		} else if (!entry->stale && !entry->complete &&
				   (!entry->refresh_after || now >= entry->refresh_after)) {
			need_refresh = SWITCH_TRUE;
		}
		if (!need_refresh && entry && !zstr(entry->prefix)) {
			stream.write_function(&stream, "%s", entry->prefix);
		}
		switch_mutex_unlock(profile->tag_var_cache_mutex);

		if (need_refresh) {
			fresh_prefix = mod_logfile_build_channel_var_prefix_from_session(profile, node->userdata, &fresh_complete);

			if (!zstr(fresh_prefix)) {
				mod_logfile_tag_var_cache_update(profile, node->userdata, fresh_prefix, fresh_complete, now);
				stream.write_function(&stream, "%s", fresh_prefix);
			} else {
				mod_logfile_tag_var_cache_note_miss(profile, node->userdata, now);

				switch_mutex_lock(profile->tag_var_cache_mutex);
				entry = switch_core_hash_find(profile->tag_var_cache, node->userdata);
				if (entry && !zstr(entry->prefix)) {
					stream.write_function(&stream, "%s", entry->prefix);
				}
				switch_mutex_unlock(profile->tag_var_cache_mutex);
			}

			switch_safe_free(fresh_prefix);
		}

		if (++profile->tag_var_cache_tick >= LOG_TAG_VAR_CACHE_SWEEP_INTERVAL) {
			profile->tag_var_cache_tick = 0;
			mod_logfile_tag_var_cache_sweep(profile, now, SWITCH_FALSE);
		}
	}

	if (zstr((char *) stream.data)) {
		switch_safe_free(stream.data);
	}

	return (char *) stream.data;
}

/* 写入一条日志节点。如有结构化前缀，则将前缀应用到日志数据的每一行 */
static switch_status_t mod_logfile_write_node(logfile_profile_t *profile, const switch_log_node_t *node)
{
	char *prefix = NULL;
	const char *p;
	switch_status_t status = SWITCH_STATUS_SUCCESS;

	if (!node || zstr(node->data)) {
		return SWITCH_STATUS_FALSE;
	}

	/* 构建结构化前缀，无前缀时直接写入原始日志 */
	prefix = mod_logfile_build_prefix(profile, node);
	if (zstr(prefix)) {
		return mod_logfile_raw_write(profile, node->data);
	}

	/* 逐行添加前缀后写入：确保多行日志的每一行都带有前缀 */
	for (p = node->data; p && *p; ) {
		const char *eol = strchr(p, '\n');
		size_t line_len = eol ? (size_t) (eol - p) : strlen(p);
		switch_stream_handle_t stream = { 0 };
		int print_len = line_len > INT_MAX ? INT_MAX : (int) line_len;

		SWITCH_STANDARD_STREAM(stream);
		stream.write_function(&stream, "%s%.*s\n", prefix, print_len, p);
		status = mod_logfile_raw_write(profile, (char *) stream.data);
		switch_safe_free(stream.data);

		if (status != SWITCH_STATUS_SUCCESS || !eol) {
			break;
		}

		p = eol + 1;
	}

	switch_safe_free(prefix);
	return status;
}

/* 处理一条日志节点：遍历所有 Profile，按级别映射规则判断是否写入。
 * 匹配优先级：all → 文件名 → 函数名 → "文件名:函数名" */
static switch_status_t process_node(const switch_log_node_t *node, switch_log_level_t level)
{
	switch_hash_index_t *hi;
	void *val;
	const void *var;
	logfile_profile_t *profile;

	for (hi = switch_core_hash_first(profile_hash); hi; hi = switch_core_hash_next(&hi)) {
		size_t mask = 0;
		size_t ok = 0;

		switch_core_hash_this(hi, &var, NULL, &val);
		profile = val;

		ok = switch_log_check_mask(profile->all_level, level);

		if (!ok) {
			mask = (size_t) switch_core_hash_find(profile->log_hash, node->file);
			ok = switch_log_check_mask(mask, level);
		}

		if (!ok) {
			mask = (size_t) switch_core_hash_find(profile->log_hash, node->func);
			ok = switch_log_check_mask(mask, level);
		}

		if (!ok) {
			char tmp[256] = "";
			switch_snprintf(tmp, sizeof(tmp), "%s:%s", node->file, node->func);
			mask = (size_t) switch_core_hash_find(profile->log_hash, tmp);
			ok = switch_log_check_mask(mask, level);
		}

		if (ok) {
			mod_logfile_write_node(profile, node);
		}

	}

	return SWITCH_STATUS_SUCCESS;
}

/* 日志回调函数，由 FreeSWITCH 核心调用 */
static switch_status_t mod_logfile_logger(const switch_log_node_t *node, switch_log_level_t level)
{
	return process_node(node, level);
}

/* Profile 清理回调：强制清除缓存、销毁哈希表、关闭日志文件、释放资源 */
static void cleanup_profile(void *ptr)
{
	logfile_profile_t *profile = (logfile_profile_t *) ptr;

	mod_logfile_tag_var_cache_sweep(profile, switch_micro_time_now(), SWITCH_TRUE);

	switch_core_hash_destroy(&profile->log_hash);
	switch_core_hash_destroy(&profile->tag_var_cache);
	if (profile->tag_var_map) {
		switch_event_destroy(&profile->tag_var_map);
	}
	switch_file_close(profile->log_afd);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Closing %s\n", profile->logfile);
	switch_safe_free(profile->logfile);

}

/* 从 XML 配置加载一个 Profile：解析 settings 和 mappings，打开日志文件并注册到哈希表 */
static switch_status_t load_profile(switch_xml_t xml)
{
	switch_xml_t param, settings;
	char *name = (char *) switch_xml_attr_soft(xml, "name");
	logfile_profile_t *new_profile;

	new_profile = switch_core_alloc(module_pool, sizeof(*new_profile));
	memset(new_profile, 0, sizeof(*new_profile));
	switch_core_hash_init(&(new_profile->log_hash));
	switch_core_hash_init(&(new_profile->tag_var_cache));
	new_profile->name = switch_core_strdup(module_pool, switch_str_nil(name));
	switch_mutex_init(&new_profile->tag_var_cache_mutex, SWITCH_MUTEX_NESTED, module_pool);

	/* 默认值：suffix=1，uuid=true，log_tags=false */
	new_profile->suffix = 1;
	new_profile->log_uuid = SWITCH_TRUE;
	new_profile->log_tags = SWITCH_FALSE;
	new_profile->tag_var_map = NULL;
	new_profile->tag_var_cache_size = 0;
	new_profile->tag_var_count = 0;
	new_profile->tag_var_cache_tick = 0;

	/* 解析 <settings> 下的 <param> 参数 */
	if ((settings = switch_xml_child(xml, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = param->next) {
			char *var = (char *) switch_xml_attr_soft(param, "name");
			char *val = (char *) switch_xml_attr_soft(param, "value");
			if (!strcmp(var, "logfile")) {
				new_profile->logfile = strdup(val);
			} else if (!strcmp(var, "rollover")) {
				new_profile->roll_size = switch_atoui(val);
			} else if (!strcmp(var, "maximum-rotate")) {
				new_profile->max_rot = switch_atoui(val);
				if (new_profile->max_rot == 0) {
					new_profile->max_rot = MAX_ROT;
				}
			} else if (!strcmp(var, "uuid")) {
				new_profile->log_uuid = switch_true(val);
			} else if (!strcmp(var, "log-tags")) {
				new_profile->log_tags = switch_true(val);
			} else if (!strcmp(var, "channel-vars")) {
				mod_logfile_add_tag_vars(new_profile, val);
			}
		}
	}

	/* 解析 <mappings> 下的 <map> 级别映射 */
	if ((settings = switch_xml_child(xml, "mappings"))) {
		for (param = switch_xml_child(settings, "map"); param; param = param->next) {
			char *var = (char *) switch_xml_attr_soft(param, "name");
			char *val = (char *) switch_xml_attr_soft(param, "value");

			add_mapping(new_profile, var, val);
		}
	}

	/* 未指定日志文件路径时，使用默认路径 $LOG_DIR/freeswitch.log */
	if (zstr(new_profile->logfile)) {
		char logfile[512];
		switch_snprintf(logfile, sizeof(logfile), "%s%s%s", SWITCH_GLOBAL_dirs.log_dir, SWITCH_PATH_SEPARATOR, "freeswitch.log");
		new_profile->logfile = strdup(logfile);
	}

	/* 打开日志文件，打开失败时清理并返回错误 */
	if (mod_logfile_openlogfile(new_profile, SWITCH_TRUE) != SWITCH_STATUS_SUCCESS) {
		if (new_profile->tag_var_map) {
			switch_event_destroy(&new_profile->tag_var_map);
		}
		switch_core_hash_destroy(&new_profile->log_hash);
		switch_core_hash_destroy(&new_profile->tag_var_cache);
		switch_safe_free(new_profile->logfile);
		return SWITCH_STATUS_GENERR;
	}

	/* 输出 Profile 配置摘要日志 */
	mod_logfile_log_profile_config(new_profile);

	/* 注册到全局 Profile 哈希表，并设置清理回调 */
	switch_core_hash_insert_destructor(profile_hash, new_profile->name, (void *) new_profile, cleanup_profile);
	return SWITCH_STATUS_SUCCESS;
}


/* SIGHUP 事件处理：根据 rotate-on-hup 配置决定轮转日志或重新打开文件 */
static void event_handler(switch_event_t *event)
{
	const char *sig = switch_event_get_header(event, "Trapped-Signal");
	switch_hash_index_t *hi;
	void *val;
	const void *var;
	logfile_profile_t *profile;

	if (sig && !strcmp(sig, "HUP")) {
		if (globals.rotate) {
			for (hi = switch_core_hash_first(profile_hash); hi; hi = switch_core_hash_next(&hi)) {
				switch_core_hash_this(hi, &var, NULL, &val);
				profile = val;
				mod_logfile_rotate(profile);
			}
		} else {
			switch_mutex_lock(globals.mutex);
			for (hi = switch_core_hash_first(profile_hash); hi; hi = switch_core_hash_next(&hi)) {
				switch_core_hash_this(hi, &var, NULL, &val);
				profile = val;
				switch_file_close(profile->log_afd);
				if (mod_logfile_openlogfile(profile, SWITCH_TRUE) != SWITCH_STATUS_SUCCESS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Error Re-opening Log!\n");
				}
			}
			switch_mutex_unlock(globals.mutex);
		}
	}
}

/* CHANNEL_DESTROY 事件处理：将已销毁通道对应 UUID 的缓存条目标记为过期，
 * 使其不会立即删除但不再刷新，等待后续缓存扫描时延迟清理 */
static void channel_destroy_event_handler(switch_event_t *event)
{
	switch_hash_index_t *hi;
	void *val;
	const void *var;
	logfile_profile_t *profile;
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	switch_time_t now;

	if (zstr(uuid)) {
		return;
	}

	now = switch_micro_time_now();
	for (hi = switch_core_hash_first(profile_hash); hi; hi = switch_core_hash_next(&hi)) {
		switch_core_hash_this(hi, &var, NULL, &val);
		profile = val;
		mod_logfile_tag_var_cache_mark_stale(profile, uuid, now);
	}
}

/* 模块加载入口：初始化全局状态、绑定事件、解析配置文件、注册日志回调 */
SWITCH_MODULE_LOAD_FUNCTION(mod_logfile_load)
{
	char *cf = "logfile.conf";
	switch_xml_t cfg, xml, settings, param, profiles, xprofile;

	module_pool = pool;

	memset(&globals, 0, sizeof(globals));
	switch_mutex_init(&globals.mutex, SWITCH_MUTEX_NESTED, module_pool);

	/* 如果热重载时 Profile 哈希表已存在，先销毁旧表 */
	if (profile_hash) {
		switch_core_hash_destroy(&profile_hash);
	}
	switch_core_hash_init(&profile_hash);

	/* 绑定 SIGHUP 事件，用于日志轮转或重新打开 */
	if (switch_event_bind_removable(modname, SWITCH_EVENT_TRAP, SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL, &globals.node) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Couldn't bind!\n");
		return SWITCH_STATUS_GENERR;
	}

	/* 绑定 CHANNEL_DESTROY 事件，用于将缓存条目标记为过期 */
	if (switch_event_bind_removable(modname, SWITCH_EVENT_CHANNEL_DESTROY, SWITCH_EVENT_SUBCLASS_ANY,
									channel_destroy_event_handler, NULL, &globals.destroy_node) != SWITCH_STATUS_SUCCESS) {
		switch_event_unbind(&globals.node);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Couldn't bind channel destroy handler!\n");
		return SWITCH_STATUS_GENERR;
	}

	/* 连接模块接口 */
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	/* 解析 logfile.conf.xml 配置文件 */
	if (!(xml = switch_xml_open_cfg(cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", cf);
	} else {
		/* 解析全局 <settings> 参数（如 rotate-on-hup） */
		if ((settings = switch_xml_child(cfg, "settings"))) {
			for (param = switch_xml_child(settings, "param"); param; param = param->next) {
				char *var = (char *) switch_xml_attr_soft(param, "name");
				char *val = (char *) switch_xml_attr_soft(param, "value");
				if (!strcmp(var, "rotate-on-hup")) {
					globals.rotate = switch_true(val);
				}
			}
		}

		/* 解析所有 <profile> 并逐一加载 */
		if ((profiles = switch_xml_child(cfg, "profiles"))) {
			for (xprofile = switch_xml_child(profiles, "profile"); xprofile; xprofile = xprofile->next) {
				if (load_profile(xprofile) != SWITCH_STATUS_SUCCESS) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "error loading profile.\n");
				}
			}
		}

		switch_xml_free(xml);
	}

	/* 注册日志回调，最低级别为 DEBUG（实际过滤由各 Profile 的级别映射决定） */
	switch_log_bind_logger(mod_logfile_logger, SWITCH_LOG_DEBUG, SWITCH_FALSE);

	return SWITCH_STATUS_SUCCESS;
}

/* 模块卸载入口：注销日志回调、解绑事件、销毁 Profile 哈希表 */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_logfile_shutdown)
{
	switch_log_unbind_logger(mod_logfile_logger);
	switch_event_unbind(&globals.destroy_node);
	switch_event_unbind(&globals.node);
	switch_core_hash_destroy(&profile_hash);
	return SWITCH_STATUS_SUCCESS;
}

/* For Emacs:
 * Local Variables:
 * mode:c
 * indent-tabs-mode:t
 * tab-width:4
 * c-basic-offset:4
 * End:
 * For VIM:
 * vim:set softtabstop=4 shiftwidth=4 tabstop=4 noet:
 */
