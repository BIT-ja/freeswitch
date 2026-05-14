#include "mod_rocketmq.h"
#include <errno.h>
#include <stdlib.h>
#include <stdexcept>
#include <string>
#include <vector>

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_rocketmq_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_rocketmq_load);
SWITCH_MODULE_DEFINITION(mod_rocketmq, mod_rocketmq_load, mod_rocketmq_shutdown, NULL);

mod_rocketmq_globals_t mod_rocketmq_globals;

/* Forward declarations */
static switch_bool_t mod_rocketmq_restart_producer();
bool mod_rocketmq_validate_topic(const char* topic);
bool mod_rocketmq_refresh_topic_route(const char* topic);
static switch_bool_t mod_rocketmq_parse_uint_param(const char *param_name, const char *param_value, unsigned int min_value, unsigned int max_value, unsigned int *out_value);
static switch_bool_t mod_rocketmq_acquire_producer_ref(rocketmq::DefaultMQProducer **producer_out);
static void mod_rocketmq_release_producer_ref(void);
static rocketmq::DefaultMQProducer *mod_rocketmq_create_started_producer(const mod_rocketmq_profile_t *profile);
static switch_status_t mod_rocketmq_create_config_container(switch_memory_pool_t **pool_out, switch_hash_t **hash_out);
static void mod_rocketmq_destroy_config_container(switch_memory_pool_t **pool_io, switch_hash_t **hash_io);
static mod_rocketmq_profile_t *mod_rocketmq_create_default_profile(switch_memory_pool_t *pool, switch_hash_t *profile_hash, const char *name);
static switch_status_t mod_rocketmq_bind_profile_events(mod_rocketmq_profile_t *profile);

SWITCH_STANDARD_API(rocketmq_reload)
{
    return mod_rocketmq_do_config(SWITCH_TRUE);
}

static switch_bool_t mod_rocketmq_acquire_producer_ref(rocketmq::DefaultMQProducer **producer_out)
{
	switch_bool_t ok = SWITCH_FALSE;
	if (!producer_out) {
		return SWITCH_FALSE;
	}
	*producer_out = NULL;

	switch_mutex_lock(mod_rocketmq_globals.mutex);
	if (mod_rocketmq_globals.producer && mod_rocketmq_globals.active_profile) {
		*producer_out = mod_rocketmq_globals.producer;
		mod_rocketmq_globals.in_flight_sends++;
		ok = SWITCH_TRUE;
	}
	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	return ok;
}

static void mod_rocketmq_release_producer_ref(void)
{
	switch_mutex_lock(mod_rocketmq_globals.mutex);
	if (mod_rocketmq_globals.in_flight_sends > 0) {
		mod_rocketmq_globals.in_flight_sends--;
		if (mod_rocketmq_globals.in_flight_sends == 0) {
			switch_thread_cond_signal(mod_rocketmq_globals.producer_cond);
		}
	}
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
}

static rocketmq::DefaultMQProducer *mod_rocketmq_create_started_producer(const mod_rocketmq_profile_t *profile)
{
	rocketmq::DefaultMQProducer *producer = NULL;

	if (!profile || zstr(profile->group_id) || zstr(profile->namesrv)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Invalid profile, cannot create producer\n");
		return NULL;
	}

	try {
		producer = new rocketmq::DefaultMQProducer(profile->group_id);
		producer->setNamesrvAddr(profile->namesrv);
		producer->setSendMsgTimeout(profile->send_timeout);
		producer->setRetryTimes(profile->retry_times);

		if (profile->access_key && profile->secret_key &&
			strlen(profile->access_key) > 0 && strlen(profile->secret_key) > 0) {
			producer->setSessionCredentials(profile->access_key, profile->secret_key, "");
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication enabled\n");
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication disabled\n");
		}

		producer->start();
		return producer;
	} catch (const std::exception &e) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to start RocketMQ producer: %s\n", e.what());
		if (producer) {
			try {
				producer->shutdown();
			} catch (const std::exception &) {
			}
			delete producer;
		}
		return NULL;
	}
}

/* Validate if a topic exists and has valid route info */
bool mod_rocketmq_validate_topic(const char* topic)
{
	switch_bool_t valid = SWITCH_FALSE;
	rocketmq::DefaultMQProducer *producer = NULL;

	if (!topic) {
		return false;
	}

	if (!mod_rocketmq_acquire_producer_ref(&producer)) {
		return false;
	}

	try {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Validating topic by sending test message: %s\n", topic);
		rocketmq::MQMessage test_msg(topic, "test", "test_key", "test_content");
		test_msg.setKeys("test_validation_key");
		rocketmq::SendResult send_result = producer->send(test_msg, 2000);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Topic validation successful: %s, msgId: %s\n",
						topic, send_result.getMsgId().c_str());
		valid = SWITCH_TRUE;
	} catch (const std::exception& e) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Topic validation failed for %s: %s\n", topic, e.what());
	}

	mod_rocketmq_release_producer_ref();
	return valid == SWITCH_TRUE;
}

/* Refresh topic route info */
bool mod_rocketmq_refresh_topic_route(const char* topic)
{
	if (!topic) {
		return false;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Refreshing topic route for: %s\n", topic);
	return mod_rocketmq_restart_producer() == SWITCH_TRUE;
}

/* ------------------------------
   Internal functions
   ------------------------------
*/
static switch_bool_t mod_rocketmq_restart_producer()
{
	switch_bool_t status = SWITCH_FALSE;
	rocketmq::DefaultMQProducer *old_producer = NULL;

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	if (!mod_rocketmq_globals.active_profile) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No active profile, cannot restart producer\n");
		switch_mutex_unlock(mod_rocketmq_globals.mutex);
		return SWITCH_FALSE;
	}

	while (mod_rocketmq_globals.in_flight_sends > 0) {
		switch_thread_cond_wait(mod_rocketmq_globals.producer_cond, mod_rocketmq_globals.mutex);
	}

	rocketmq::DefaultMQProducer *new_producer = mod_rocketmq_create_started_producer(mod_rocketmq_globals.active_profile);
	if (new_producer) {
		old_producer = mod_rocketmq_globals.producer;
		mod_rocketmq_globals.producer = new_producer;
		status = SWITCH_TRUE;
	}

	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	if (old_producer) {
		try {
			old_producer->shutdown();
		} catch (const std::exception &e) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error during old producer shutdown: %s\n", e.what());
		}
		delete old_producer;
	}

	if (status == SWITCH_TRUE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer restarted successfully\n");
	}

	return status;
}

static switch_status_t mod_rocketmq_create_config_container(switch_memory_pool_t **pool_out, switch_hash_t **hash_out)
{
	if (!pool_out || !hash_out) {
		return SWITCH_STATUS_FALSE;
	}

	*pool_out = NULL;
	*hash_out = NULL;

	if (switch_core_new_memory_pool(pool_out) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create config memory pool\n");
		return SWITCH_STATUS_MEMERR;
	}

	switch_core_hash_init(hash_out);
	if (!*hash_out) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to initialize profile hash\n");
		switch_core_destroy_memory_pool(pool_out);
		return SWITCH_STATUS_GENERR;
	}

	return SWITCH_STATUS_SUCCESS;
}

static void mod_rocketmq_destroy_config_container(switch_memory_pool_t **pool_io, switch_hash_t **hash_io)
{
	if (hash_io && *hash_io) {
		switch_core_hash_destroy(hash_io);
	}
	if (pool_io && *pool_io) {
		switch_core_destroy_memory_pool(pool_io);
	}
}

static mod_rocketmq_profile_t *mod_rocketmq_create_default_profile(switch_memory_pool_t *pool, switch_hash_t *profile_hash, const char *name)
{
	mod_rocketmq_profile_t *profile = NULL;
	const char *profile_name = zstr(name) ? "default" : name;

	profile = (mod_rocketmq_profile_t *)switch_core_alloc(pool, sizeof(mod_rocketmq_profile_t));
	memset(profile, 0, sizeof(*profile));

	profile->name = switch_core_strdup(pool, profile_name);
	profile->namesrv = switch_core_strdup(pool, "localhost:9876");
	profile->group_id = switch_core_strdup(pool, "freeswitch_producer_group");
	profile->topic = switch_core_strdup(pool, "freeswitch_events");
	profile->tag = switch_core_strdup(pool, "event");
	profile->filter_account_prefix = switch_core_strdup(pool, "BYTEL");
	profile->send_timeout = 3000;
	profile->retry_times = 3;
	profile->max_message_size = 4096;
	profile->reconnect_retry_times = 5;
	profile->reconnect_retry_interval = 1000;
	profile->topic_queue_count = 4;
	profile->message_mode = switch_core_strdup(pool, "ordered");
	profile->topic_validation_mode = switch_core_strdup(pool, "none");
	profile->events[0].id = SWITCH_EVENT_ALL;
	profile->events[0].subclass = SWITCH_EVENT_SUBCLASS_ANY;
	profile->event_subscriptions = 1;

	switch_core_hash_insert(profile_hash, profile->name, profile);
	return profile;
}

static switch_status_t mod_rocketmq_bind_profile_events(mod_rocketmq_profile_t *profile)
{
	int i;

	if (!profile) {
		return SWITCH_STATUS_FALSE;
	}

	for (i = 0; i < profile->event_subscriptions; i++) {
		if (switch_event_bind_removable("ROCKETMQ",
										profile->events[i].id,
										profile->events[i].subclass,
										mod_rocketmq_event_handler,
										NULL,
										&(profile->event_nodes[i])) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind event handler %d\n", (int)profile->events[i].id);
			break;
		}
	}

	if (i != profile->event_subscriptions) {
		int j;
		for (j = 0; j < i; j++) {
			if (profile->event_nodes[j]) {
				switch_event_unbind(&profile->event_nodes[j]);
			}
		}
		return SWITCH_STATUS_GENERR;
	}

	return SWITCH_STATUS_SUCCESS;
}

static switch_bool_t mod_rocketmq_parse_uint_param(const char *param_name, const char *param_value, unsigned int min_value, unsigned int max_value, unsigned int *out_value)
{
	char *end = NULL;
	unsigned long parsed = 0;

	if (zstr(param_value) || !out_value) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Invalid value for %s: empty\n", param_name);
		return SWITCH_FALSE;
	}

	errno = 0;
	parsed = strtoul(param_value, &end, 10);
	if (errno != 0 || end == param_value || *end != '\0') {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Invalid value for %s: %s\n", param_name, param_value);
		return SWITCH_FALSE;
	}

	if (parsed < min_value || parsed > max_value) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Out-of-range value for %s: %s (allowed %u-%u)\n",
						param_name, param_value, min_value, max_value);
		return SWITCH_FALSE;
	}

	*out_value = (unsigned int)parsed;
	return SWITCH_TRUE;
}

/* ------------------------------
   Event handler
   ------------------------------
*/
void mod_rocketmq_event_handler(switch_event_t* evt)
{
	mod_rocketmq_message_t msg = { 0 };
	char *json_str = NULL;
	const char *event_name = NULL;
	const char *prefix = "";
	std::string filter_account_prefix;
	std::string message_mode;
	unsigned int reconnect_retry_times = 0;
	unsigned int reconnect_retry_interval = 0;
	unsigned int max_message_size = 0;
	unsigned int topic_queue_count = 1;
	switch_bool_t runtime_ready = SWITCH_FALSE;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "RocketMQ event handler called\n");

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	if (mod_rocketmq_globals.producer && mod_rocketmq_globals.active_profile) {
		if (mod_rocketmq_globals.active_profile->topic) {
			strncpy(msg.topic, mod_rocketmq_globals.active_profile->topic, ROCKETMQ_MAX_TOPIC_LENGTH - 1);
		}
		if (mod_rocketmq_globals.active_profile->tag) {
			strncpy(msg.tag, mod_rocketmq_globals.active_profile->tag, ROCKETMQ_MAX_TAG_LENGTH - 1);
		}
		if (mod_rocketmq_globals.active_profile->filter_account_prefix) {
			filter_account_prefix.assign(mod_rocketmq_globals.active_profile->filter_account_prefix);
		}
		if (mod_rocketmq_globals.active_profile->message_mode) {
			message_mode.assign(mod_rocketmq_globals.active_profile->message_mode);
		}
		reconnect_retry_times = mod_rocketmq_globals.active_profile->reconnect_retry_times;
		reconnect_retry_interval = mod_rocketmq_globals.active_profile->reconnect_retry_interval;
		max_message_size = mod_rocketmq_globals.active_profile->max_message_size;
		topic_queue_count = mod_rocketmq_globals.active_profile->topic_queue_count ? mod_rocketmq_globals.active_profile->topic_queue_count : 1;
		runtime_ready = SWITCH_TRUE;
	}

	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	if (!runtime_ready) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "No active RocketMQ producer, skipping event\n");
		goto done;
	}

	prefix = filter_account_prefix.c_str();

	event_name = switch_event_get_header(evt, "Event-Name");
	if (!event_name || strlen(event_name) == 0) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring event: Event-Name is empty\n");
		goto done;
	}

	if (strcmp(event_name, "CUSTOM") == 0) {
		const char *sip_auth_username = switch_event_get_header(evt, "sip_auth_username");

		if (!sip_auth_username || strlen(sip_auth_username) == 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring CUSTOM event: sip_auth_username is empty\n");
			goto done;
		}

		if (strlen(prefix) > 0) {
			if (strncmp(sip_auth_username, prefix, strlen(prefix)) == 0) {
				snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", sip_auth_username);
			} else {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring CUSTOMER::'%s' event: sip_auth_username '%s' doesn't match configured prefix '%s'\n", 
								switch_event_get_header(evt, "Event-Subclass"), sip_auth_username, prefix);
				goto done;
			}
		} else {
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", sip_auth_username);
		}
	} else if (strncmp(event_name, "CHANNEL_", 8) == 0) {
		const char *channel_name = switch_event_get_header(evt, "Channel-Name");
		if (!channel_name) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring '%s' event: Channel-Name is empty\n", event_name);
			goto done;
		}

		std::string expected_prefix = std::string("sofia/internal/") + prefix;
		if (strncmp(channel_name, expected_prefix.c_str(), expected_prefix.size()) != 0) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring '%s' event: Channel-Name not filter: %s\n", event_name, channel_name);
			goto done;
		}

		const char *start_pos = channel_name + strlen("sofia/internal/");
		const char *end_pos = strchr(start_pos, '@');
		if (end_pos != NULL) {
			size_t extracted_len = end_pos - start_pos;
			std::string extracted_part(start_pos, extracted_len);
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", extracted_part.c_str());
		}
	} else {
		const char *core_uuid = switch_event_get_header(evt, "Core-UUID");
		const char *event_seq = switch_event_get_header(evt, "Event-Sequence");
		if (core_uuid && event_seq) {
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s_%s_%s", event_name, core_uuid, event_seq);
		} else if (core_uuid) {
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s_%s", event_name, core_uuid);
		} else {
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s_%llu", event_name, (unsigned long long)switch_epoch_time_now(NULL));
		}
	}

	if (switch_event_serialize_json(evt, &json_str) == SWITCH_STATUS_SUCCESS) {
		msg.pjson = json_str;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Generated JSON: %s\n", msg.pjson);
		if (strlen(msg.pjson) > max_message_size) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Dropping event: message size %zu exceeds max_message_size %u\n",
							strlen(msg.pjson), max_message_size);
			goto done;
		}

		unsigned int send_attempt = 0;
		switch_bool_t success = SWITCH_FALSE;

		while (!success && send_attempt <= reconnect_retry_times) {
			rocketmq::DefaultMQProducer *producer = NULL;
			if (!mod_rocketmq_acquire_producer_ref(&producer)) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No active RocketMQ producer, aborting send\n");
				break;
			}

			try {
				rocketmq::MQMessage rocketmq_msg(msg.topic, msg.tag, msg.pjson);
				if (strlen(msg.key) > 0) {
					rocketmq_msg.setKeys(msg.key);
				}

				rocketmq::SendResult send_result;

				if (strcmp(message_mode.c_str(), "ordered") == 0) {
					uint32_t hash = 0;
					for (const char *p = msg.key; *p; p++) {
						hash = hash * 31 + *p;
					}

					int queue_count = (int)topic_queue_count;
					int queue_id = hash % queue_count;

					rocketmq_msg.setKeys(msg.key);

					class SimpleQueueSelector : public rocketmq::MessageQueueSelector {
					public:
						virtual rocketmq::MQMessageQueue select(const std::vector<rocketmq::MQMessageQueue>& mqs,
																const rocketmq::MQMessage& msg,
																void* arg) {
							if (mqs.empty()) {
								throw std::runtime_error("No message queues available");
							}
							int* queue_id = static_cast<int*>(arg);
							int actual_queue_id = *queue_id % mqs.size();
							if (actual_queue_id < 0) {
								actual_queue_id = 0;
							}
							return mqs[actual_queue_id];
						}
					} selector;

					send_result = producer->send(rocketmq_msg, &selector, &queue_id);
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Sent ordered message to RocketMQ: topic=%s, tag=%s, key=%s, calculatedQueueId=%d, msgId=%s\n", 
									msg.topic, msg.tag, msg.key, queue_id, send_result.getMsgId().c_str());
				} else {
					send_result = producer->send(rocketmq_msg);
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Sent unordered message to RocketMQ: topic=%s, tag=%s, key=%s, msgId=%s\n", 
									msg.topic, msg.tag, msg.key, send_result.getMsgId().c_str());
				}

				success = SWITCH_TRUE;
			} catch (const std::exception& e) {
				send_attempt++;
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to send message to RocketMQ (attempt %u/%u): Topic=%s, Tag=%s, Key=%s, Error: %s\n",
								send_attempt, reconnect_retry_times, msg.topic, msg.tag, msg.key, e.what());

				std::string error_str(e.what());
				bool is_route_error = (error_str.find("No route info for this topic") != std::string::npos);

				if (is_route_error) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Route error detected for topic: %s\n", msg.topic);
				}
			}

			mod_rocketmq_release_producer_ref();

			if (!success) {
				if (send_attempt <= reconnect_retry_times) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Restarting RocketMQ producer and retrying...\n");
					if (mod_rocketmq_restart_producer()) {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Waiting %u ms before retry...\n", reconnect_retry_interval);
						switch_yield(reconnect_retry_interval * 1000);
					} else {
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to restart producer, giving up\n");
						break;
					}
				} else {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Max retry attempts reached, giving up\n");
				}
			}
		}
	} else {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to serialize event to JSON\n");
	}

done:
	if (json_str) {
		free(json_str);
	}
}

/* ------------------------------
   Configuration functions
   ------------------------------
*/
switch_status_t mod_rocketmq_do_config(switch_bool_t reload)
{
	switch_xml_t cfg = NULL, xml = NULL, profiles_node = NULL, profile_node = NULL, param_node = NULL;
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	switch_memory_pool_t *new_profile_pool = NULL;
	switch_hash_t *new_profile_hash = NULL;
	mod_rocketmq_profile_t *new_active_profile = NULL;
	switch_memory_pool_t *old_profile_pool = NULL;
	switch_hash_t *old_profile_hash = NULL;
	mod_rocketmq_profile_t *old_active_profile = NULL;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Loading RocketMQ configuration...\n");

	if (mod_rocketmq_create_config_container(&new_profile_pool, &new_profile_hash) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_GENERR;
	}

	xml = switch_xml_open_cfg("rocketmq.conf", &cfg, NULL);
	if (!xml || !cfg) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open rocketmq.conf.xml\n");
		goto parse_done;
	}

	profiles_node = switch_xml_child(cfg, "profiles");
	if (!profiles_node) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "No profiles node found in rocketmq.conf.xml\n");
		goto parse_done;
	}

	for (profile_node = switch_xml_child(profiles_node, "profile"); profile_node; profile_node = profile_node->next) {
		const char *name = switch_xml_attr_soft(profile_node, "name");
		switch_bool_t has_send_timeout = SWITCH_FALSE;
		switch_bool_t has_retry_times = SWITCH_FALSE;
		switch_bool_t has_max_message_size = SWITCH_FALSE;
		switch_bool_t has_reconnect_retry_times = SWITCH_FALSE;
		switch_bool_t has_reconnect_retry_interval = SWITCH_FALSE;
		switch_bool_t has_topic_queue_count = SWITCH_FALSE;
		mod_rocketmq_profile_t *profile = NULL;

		if (zstr(name)) {
			continue;
		}

		profile = (mod_rocketmq_profile_t *)switch_core_alloc(new_profile_pool, sizeof(mod_rocketmq_profile_t));
		memset(profile, 0, sizeof(*profile));
		profile->name = switch_core_strdup(new_profile_pool, name);
		profile->events[0].id = SWITCH_EVENT_ALL;
		profile->events[0].subclass = SWITCH_EVENT_SUBCLASS_ANY;
		profile->event_subscriptions = 1;

		switch_core_hash_insert(new_profile_hash, profile->name, profile);

		for (param_node = switch_xml_child(profile_node, "param"); param_node; param_node = param_node->next) {
			const char *param_name = switch_xml_attr_soft(param_node, "name");
			const char *param_value = switch_xml_attr_soft(param_node, "value");

			if (!param_name || !param_value) {
				continue;
			}

			if (!strcasecmp(param_name, "namesrv")) {
				profile->namesrv = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "group_id")) {
				profile->group_id = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "access_key")) {
				profile->access_key = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "secret_key")) {
				profile->secret_key = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "topic")) {
				profile->topic = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "tag")) {
				profile->tag = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "filter_account_prefix")) {
				profile->filter_account_prefix = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "event_filter")) {
				char *dup_tmp = switch_core_strdup(new_profile_pool, param_value);
				char *argv[SWITCH_EVENT_ALL];
				int arg, token_count, valid_count = 0;

				token_count = switch_separate_string(dup_tmp, ',', argv, (sizeof(argv) / sizeof(argv[0])));
				for (arg = 0; arg < token_count; arg++) {
					char *subclass = (char *)SWITCH_EVENT_SUBCLASS_ANY;
					char *event_copy = switch_core_strdup(new_profile_pool, argv[arg]);
					char *trimmed_event = event_copy;
					char *end;

					while (*trimmed_event && isspace((unsigned char)*trimmed_event)) {
						trimmed_event++;
					}
					if (*trimmed_event == '\0') {
						continue;
					}
					end = trimmed_event + strlen(trimmed_event) - 1;
					while (end > trimmed_event && isspace((unsigned char)*end)) {
						end--;
					}
					*(end + 1) = '\0';
					if (*trimmed_event == '\0') {
						continue;
					}

					if ((subclass = strchr(trimmed_event, '^'))) {
						*subclass++ = '\0';

						end = trimmed_event + strlen(trimmed_event) - 1;
						while (end > trimmed_event && isspace((unsigned char)*end)) {
							end--;
						}
						*(end + 1) = '\0';

						while (*subclass && isspace((unsigned char)*subclass)) {
							subclass++;
						}
						if (*subclass != '\0') {
							end = subclass + strlen(subclass) - 1;
							while (end > subclass && isspace((unsigned char)*end)) {
								end--;
							}
							*(end + 1) = '\0';
						} else {
							subclass = (char *)SWITCH_EVENT_SUBCLASS_ANY;
						}
					}

					if (*trimmed_event == '\0') {
						continue;
					}

					if (switch_name_event(trimmed_event, &(profile->events[valid_count].id)) == SWITCH_STATUS_SUCCESS) {
						profile->events[valid_count].subclass = subclass;
						valid_count++;
					}
				}

				if (valid_count > 0) {
					profile->event_subscriptions = valid_count;
				}
			} else if (!strcasecmp(param_name, "reconnect_retry_times")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 0, 1000, &value)) {
					profile->reconnect_retry_times = value;
					has_reconnect_retry_times = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "reconnect_retry_interval")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 1, 600000, &value)) {
					profile->reconnect_retry_interval = value;
					has_reconnect_retry_interval = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "send_timeout")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 1, 600000, &value)) {
					profile->send_timeout = value;
					has_send_timeout = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "retry_times")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 0, 100, &value)) {
					profile->retry_times = value;
					has_retry_times = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "max_message_size")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 1, 4194304, &value)) {
					profile->max_message_size = value;
					has_max_message_size = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "message_mode")) {
				profile->message_mode = switch_core_strdup(new_profile_pool, param_value);
			} else if (!strcasecmp(param_name, "topic_queue_count")) {
				unsigned int value = 0;
				if (mod_rocketmq_parse_uint_param(param_name, param_value, 1, 65535, &value)) {
					profile->topic_queue_count = value;
					has_topic_queue_count = SWITCH_TRUE;
				}
			} else if (!strcasecmp(param_name, "topic_validation_mode")) {
				profile->topic_validation_mode = switch_core_strdup(new_profile_pool, param_value);
			}
		}

		if (!profile->namesrv) {
			profile->namesrv = switch_core_strdup(new_profile_pool, "localhost:9876");
		}
		if (!profile->group_id) {
			profile->group_id = switch_core_strdup(new_profile_pool, "freeswitch_producer_group");
		}
		if (!profile->topic) {
			profile->topic = switch_core_strdup(new_profile_pool, "freeswitch_events");
		}
		if (!profile->tag) {
			profile->tag = switch_core_strdup(new_profile_pool, "event");
		}
		if (!profile->filter_account_prefix) {
			profile->filter_account_prefix = switch_core_strdup(new_profile_pool, "BYTEL");
		}
		if (!has_send_timeout) {
			profile->send_timeout = 3000;
		}
		if (!has_retry_times) {
			profile->retry_times = 3;
		}
		if (!has_max_message_size) {
			profile->max_message_size = 4096;
		}
		if (!has_reconnect_retry_times) {
			profile->reconnect_retry_times = 5;
		}
		if (!has_reconnect_retry_interval) {
			profile->reconnect_retry_interval = 1000;
		}
		if (!profile->message_mode) {
			profile->message_mode = switch_core_strdup(new_profile_pool, "ordered");
		}
		if (!has_topic_queue_count) {
			profile->topic_queue_count = 4;
		}
		if (!profile->topic_validation_mode) {
			profile->topic_validation_mode = switch_core_strdup(new_profile_pool, "none");
		}
		if (!strcasecmp(profile->message_mode, "ordered")) {
			profile->message_mode = switch_core_strdup(new_profile_pool, "ordered");
		} else if (!strcasecmp(profile->message_mode, "unordered")) {
			profile->message_mode = switch_core_strdup(new_profile_pool, "unordered");
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Invalid message_mode '%s', fallback to ordered\n", profile->message_mode);
			profile->message_mode = switch_core_strdup(new_profile_pool, "ordered");
		}
		if (!strcasecmp(profile->topic_validation_mode, "none")) {
			profile->topic_validation_mode = switch_core_strdup(new_profile_pool, "none");
		} else if (!strcasecmp(profile->topic_validation_mode, "send_test_message")) {
			profile->topic_validation_mode = switch_core_strdup(new_profile_pool, "send_test_message");
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Invalid topic_validation_mode '%s', fallback to none\n", profile->topic_validation_mode);
			profile->topic_validation_mode = switch_core_strdup(new_profile_pool, "none");
		}

		if (!new_active_profile) {
			new_active_profile = profile;
		}
	}

parse_done:
	if (xml) {
		switch_xml_free(xml);
		xml = NULL;
	}

	if (!new_active_profile) {
		if (reload) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Reload aborted: no valid profile in rocketmq.conf.xml\n");
			status = SWITCH_STATUS_GENERR;
			goto done;
		}

		new_active_profile = mod_rocketmq_create_default_profile(new_profile_pool, new_profile_hash, "default");
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Using built-in default RocketMQ profile\n");
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Current RocketMQ configuration:\n");
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Active profile: %s\n", new_active_profile->name);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Namesrv: %s\n", new_active_profile->namesrv);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Group ID: %s\n", new_active_profile->group_id);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Access Key: %s\n", new_active_profile->access_key ? "set" : "not set");
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Secret Key: %s\n", new_active_profile->secret_key ? "set" : "not set");
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Topic: %s\n", new_active_profile->topic);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Tag: %s\n", new_active_profile->tag);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Filter Account Prefix: %s\n", new_active_profile->filter_account_prefix);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Send timeout: %u ms\n", new_active_profile->send_timeout);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Retry times: %u\n", new_active_profile->retry_times);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Max message size: %u bytes\n", new_active_profile->max_message_size);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Reconnect retry times: %u\n", new_active_profile->reconnect_retry_times);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Reconnect retry interval: %u ms\n", new_active_profile->reconnect_retry_interval);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Topic queue count: %u\n", new_active_profile->topic_queue_count);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Message mode: %s\n", new_active_profile->message_mode);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Topic validation mode: %s\n", new_active_profile->topic_validation_mode);

	if (!reload) {
		switch_mutex_lock(mod_rocketmq_globals.mutex);
		mod_rocketmq_globals.profile_pool = new_profile_pool;
		mod_rocketmq_globals.profile_hash = new_profile_hash;
		mod_rocketmq_globals.active_profile = new_active_profile;
		switch_mutex_unlock(mod_rocketmq_globals.mutex);

		new_profile_pool = NULL;
		new_profile_hash = NULL;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ configuration loaded successfully\n");
		return SWITCH_STATUS_SUCCESS;
	}

	switch_mutex_lock(mod_rocketmq_globals.mutex);
	old_profile_pool = mod_rocketmq_globals.profile_pool;
	old_profile_hash = mod_rocketmq_globals.profile_hash;
	old_active_profile = mod_rocketmq_globals.active_profile;
	mod_rocketmq_globals.profile_pool = new_profile_pool;
	mod_rocketmq_globals.profile_hash = new_profile_hash;
	mod_rocketmq_globals.active_profile = new_active_profile;
	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	switch_event_unbind_callback(mod_rocketmq_event_handler);
	if (mod_rocketmq_bind_profile_events(mod_rocketmq_globals.active_profile) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Reload failed: cannot bind new event subscriptions\n");
		status = SWITCH_STATUS_GENERR;
		goto rollback;
	}

	if (mod_rocketmq_restart_producer() != SWITCH_TRUE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Reload failed: cannot restart producer with new config\n");
		status = SWITCH_STATUS_GENERR;
		goto rollback;
	}

	mod_rocketmq_destroy_config_container(&old_profile_pool, &old_profile_hash);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ configuration reloaded successfully\n");
	return SWITCH_STATUS_SUCCESS;

rollback:
	switch_event_unbind_callback(mod_rocketmq_event_handler);
	switch_mutex_lock(mod_rocketmq_globals.mutex);
	mod_rocketmq_globals.profile_pool = old_profile_pool;
	mod_rocketmq_globals.profile_hash = old_profile_hash;
	mod_rocketmq_globals.active_profile = old_active_profile;
	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	if (old_active_profile) {
		if (mod_rocketmq_bind_profile_events(old_active_profile) != SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Rollback failed: cannot restore old event bindings\n");
		}
	}

done:
	mod_rocketmq_destroy_config_container(&new_profile_pool, &new_profile_hash);
	return status;
}

/* ------------------------------
   Startup
   ------------------------------
*/
SWITCH_MODULE_LOAD_FUNCTION(mod_rocketmq_load)
{
	switch_api_interface_t *api_interface;
	switch_status_t status = SWITCH_STATUS_GENERR;
	rocketmq::DefaultMQProducer *producer = NULL;
	switch_memory_pool_t *profile_pool = NULL;
	switch_hash_t *profile_hash = NULL;

	memset(&mod_rocketmq_globals, 0, sizeof(mod_rocketmq_globals_t));
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	mod_rocketmq_globals.pool = pool;
	switch_mutex_init(&(mod_rocketmq_globals.mutex), SWITCH_MUTEX_NESTED, pool);
	switch_thread_cond_create(&(mod_rocketmq_globals.producer_cond), pool);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq loading: Version %s\n", switch_version_full());

	if (mod_rocketmq_do_config(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to load RocketMQ configuration\n");
		goto fail;
	}

	if (!mod_rocketmq_globals.active_profile) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No active profile after configuration\n");
		goto fail;
	}

	if (mod_rocketmq_restart_producer() != SWITCH_TRUE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to initialize RocketMQ producer\n");
		goto fail;
	}

	if (!strcasecmp(mod_rocketmq_globals.active_profile->topic_validation_mode, "send_test_message")) {
		if (!mod_rocketmq_validate_topic(mod_rocketmq_globals.active_profile->topic)) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Topic validation failed, trying producer refresh\n");
			if (!mod_rocketmq_refresh_topic_route(mod_rocketmq_globals.active_profile->topic) ||
				!mod_rocketmq_validate_topic(mod_rocketmq_globals.active_profile->topic)) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Topic validation failed after refresh\n");
				goto fail;
			}
		}
	} else {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Topic validation skipped (topic_validation_mode=%s)\n",
						mod_rocketmq_globals.active_profile->topic_validation_mode);
	}

	if (mod_rocketmq_bind_profile_events(mod_rocketmq_globals.active_profile) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to bind event handlers\n");
		goto fail;
	}

	SWITCH_ADD_API(api_interface, "rocketmq", "rocketmq API", rocketmq_reload, "syntax");
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq loaded successfully\n");
	return SWITCH_STATUS_SUCCESS;

fail:
	switch_event_unbind_callback(mod_rocketmq_event_handler);
	switch_mutex_lock(mod_rocketmq_globals.mutex);
	while (mod_rocketmq_globals.in_flight_sends > 0) {
		switch_thread_cond_wait(mod_rocketmq_globals.producer_cond, mod_rocketmq_globals.mutex);
	}
	producer = mod_rocketmq_globals.producer;
	mod_rocketmq_globals.producer = NULL;
	profile_pool = mod_rocketmq_globals.profile_pool;
	profile_hash = mod_rocketmq_globals.profile_hash;
	mod_rocketmq_globals.profile_pool = NULL;
	mod_rocketmq_globals.profile_hash = NULL;
	mod_rocketmq_globals.active_profile = NULL;
	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	if (producer) {
		try {
			producer->shutdown();
		} catch (const std::exception &) {
		}
		delete producer;
	}

	mod_rocketmq_destroy_config_container(&profile_pool, &profile_hash);
	return status;
}

/* ------------------------------
   Shutdown
   ------------------------------
*/
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_rocketmq_shutdown)
{
	/* Unbind event handler */
	switch_event_unbind_callback(mod_rocketmq_event_handler);

	rocketmq::DefaultMQProducer *producer = NULL;
	switch_memory_pool_t *profile_pool = NULL;
	switch_hash_t *profile_hash = NULL;

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	while (mod_rocketmq_globals.in_flight_sends > 0) {
		switch_thread_cond_wait(mod_rocketmq_globals.producer_cond, mod_rocketmq_globals.mutex);
	}

	producer = mod_rocketmq_globals.producer;
	mod_rocketmq_globals.producer = NULL;
	profile_pool = mod_rocketmq_globals.profile_pool;
	profile_hash = mod_rocketmq_globals.profile_hash;
	mod_rocketmq_globals.profile_pool = NULL;
	mod_rocketmq_globals.profile_hash = NULL;
	mod_rocketmq_globals.active_profile = NULL;

	switch_mutex_unlock(mod_rocketmq_globals.mutex);

	if (producer) {
		try {
			producer->shutdown();
		} catch (const std::exception &e) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error during producer shutdown: %s\n", e.what());
		}
		delete producer;
	}

	mod_rocketmq_destroy_config_container(&profile_pool, &profile_hash);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq shutdown complete\n");
	return SWITCH_STATUS_SUCCESS;
}
