/* Force old C++ ABI for compatibility with RocketMQ library */
#define _GLIBCXX_USE_CXX11_ABI 0

#include "mod_rocketmq.h"
#include <errno.h>
#include <stdlib.h>

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_rocketmq_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_rocketmq_load);
SWITCH_MODULE_DEFINITION(mod_rocketmq, mod_rocketmq_load, mod_rocketmq_shutdown, NULL);

mod_rocketmq_globals_t mod_rocketmq_globals;

/* Forward declarations */
static switch_bool_t mod_rocketmq_restart_producer();
bool mod_rocketmq_validate_topic(const char* topic);
bool mod_rocketmq_refresh_topic_route(const char* topic);
static void mod_rocketmq_destroy_old_profiles();
static switch_bool_t mod_rocketmq_parse_uint_param(const char *param_name, const char *param_value, unsigned int min_value, unsigned int max_value, unsigned int *out_value);

SWITCH_STANDARD_API(rocketmq_reload)
{
    return mod_rocketmq_do_config(SWITCH_TRUE);
}

/* Validate if a topic exists and has valid route info */
bool mod_rocketmq_validate_topic(const char* topic)
{
	switch_bool_t valid = SWITCH_FALSE;

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	if (!topic || !mod_rocketmq_globals.producer) {
		goto done;
	}
	
	try {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Validating topic: %s\n", topic);
		
		// Try to send a small test message to validate the topic
		rocketmq::MQMessage test_msg(topic, "test", "test_key", "test_content");
		test_msg.setKeys("test_validation_key");
		
		// Short timeout for validation
		rocketmq::SendResult send_result = mod_rocketmq_globals.producer->send(test_msg, 2000);
		
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Topic validation successful: %s, msgId: %s\n",
						topic, send_result.getMsgId().c_str());
		valid = SWITCH_TRUE;
	} catch (const std::exception& e) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Topic validation failed for %s: %s\n", topic, e.what());
	}

done:
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
	return valid == SWITCH_TRUE;
}

/* Refresh topic route info */
bool mod_rocketmq_refresh_topic_route(const char* topic)
{
	if (!topic) {
		return false;
	}
    
    try {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Refreshing topic route for: %s\n", topic);
        
        // Restart producer to force route refresh
        return mod_rocketmq_restart_producer();
    } catch (const std::exception& e) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to refresh topic route for %s: %s\n", topic, e.what());
        return false;
    }
}

/* ------------------------------
   Internal functions
   ------------------------------
*/
static switch_bool_t mod_rocketmq_restart_producer()
{
	switch_bool_t status = SWITCH_FALSE;

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	if (!mod_rocketmq_globals.active_profile) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No active profile, cannot restart producer\n");
		goto done;
	}
	
	try {
        /* Stop and delete existing producer */
        if (mod_rocketmq_globals.producer) {
            try {
                mod_rocketmq_globals.producer->shutdown();
            } catch (const std::exception &e) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error during producer shutdown: %s\n", e.what());
            }
            delete mod_rocketmq_globals.producer;
            mod_rocketmq_globals.producer = NULL;
        }
        
        /* Create new producer */
        mod_rocketmq_globals.producer = new rocketmq::DefaultMQProducer(mod_rocketmq_globals.active_profile->group_id);
        
        /* Set producer properties */
        mod_rocketmq_globals.producer->setNamesrvAddr(mod_rocketmq_globals.active_profile->namesrv);
        mod_rocketmq_globals.producer->setSendMsgTimeout(mod_rocketmq_globals.active_profile->send_timeout);
        mod_rocketmq_globals.producer->setRetryTimes(mod_rocketmq_globals.active_profile->retry_times);
        
        /* Set authentication if access_key and secret_key are provided */
        if (mod_rocketmq_globals.active_profile->access_key && mod_rocketmq_globals.active_profile->secret_key &&
            strlen(mod_rocketmq_globals.active_profile->access_key) > 0 && strlen(mod_rocketmq_globals.active_profile->secret_key) > 0) {
            mod_rocketmq_globals.producer->setSessionCredentials(mod_rocketmq_globals.active_profile->access_key,
                                                                mod_rocketmq_globals.active_profile->secret_key,
                                                                "");
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication enabled\n");
        } else {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication disabled\n");
        }
        
		/* Start producer */
		mod_rocketmq_globals.producer->start();
		
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer restarted successfully\n");
		status = SWITCH_TRUE;
	} catch (const std::exception &e) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to restart RocketMQ producer: %s\n", e.what());
        /* Clean up on failure */
        if (mod_rocketmq_globals.producer) {
			delete mod_rocketmq_globals.producer;
			mod_rocketmq_globals.producer = NULL;
		}
	}

done:
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
	return status;
}

/* Destroy old profiles during reload */
static void mod_rocketmq_destroy_old_profiles()
{
    /* Reset profile hash */
    switch_core_hash_destroy(&(mod_rocketmq_globals.profile_hash));
    switch_core_hash_init(&(mod_rocketmq_globals.profile_hash));
    
    /* Reset active profile */
	mod_rocketmq_globals.active_profile = NULL;
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
	const char *prefix = NULL;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "RocketMQ event handler called\n");

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	// Check if we have an active producer
	if (!mod_rocketmq_globals.producer || !mod_rocketmq_globals.active_profile) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "No active RocketMQ producer, skipping event\n");
		goto done;
	}

	// Set topic and tag from configuration
	if (mod_rocketmq_globals.active_profile->topic) {
		strncpy(msg.topic, mod_rocketmq_globals.active_profile->topic, ROCKETMQ_MAX_TOPIC_LENGTH - 1);
	}
	if (mod_rocketmq_globals.active_profile->tag) {
		strncpy(msg.tag, mod_rocketmq_globals.active_profile->tag, ROCKETMQ_MAX_TAG_LENGTH - 1);
	}

	// Generate message key using event information
	event_name = switch_event_get_header(evt, "Event-Name");
	
	// Check if event_name is empty
	if (!event_name || strlen(event_name) == 0) {
		// Ignore event if event_name is empty
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring event: Event-Name is empty\n");
		goto done;
	}

	prefix = mod_rocketmq_globals.active_profile->filter_account_prefix ? mod_rocketmq_globals.active_profile->filter_account_prefix : "";

	// Special handling for different event types
	if (strcmp(event_name, "CUSTOM") == 0) {
		const char *sip_auth_username = switch_event_get_header(evt, "sip_auth_username");
		// Check if sip_auth_username is empty for CUSTOM events
		if (!sip_auth_username || strlen(sip_auth_username) == 0) {
			// Ignore CUSTOM event if sip_auth_username is empty
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring CUSTOM event: sip_auth_username is empty\n");
			goto done;
		}
		
		// Check if sip_auth_username starts with configured prefix
		if (prefix && strlen(prefix) > 0) {
			if (strncmp(sip_auth_username, prefix, strlen(prefix)) == 0) {
				snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", sip_auth_username);
			} else {
				// Ignore messages with non-configured prefix
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring CUSTOMER::'%s' event: sip_auth_username '%s' doesn't match configured prefix '%s'\n", 
								switch_event_get_header(evt, "Event-Subclass"), sip_auth_username, prefix);
				goto done;
			}
		} else {
			// If no prefix configured, use sip_auth_username as key
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", sip_auth_username);
		}
	}
	// 处理CHANNEL_开头的通话事件
	else if (strncmp(event_name, "CHANNEL_", 8) == 0) {
		const char *channel_name = switch_event_get_header(evt, "Channel-Name");
        // 检查Channel-Name是否为空
		if (!channel_name) {
			// Ignore if Channel-Name is empty
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring '%s' event: Channel-Name is empty\n", event_name);
			goto done;
		}
		size_t prefix_len = strlen(prefix);
		size_t expected_prefix_len = strlen("sofia/internal/") + prefix_len + 1; // +1 for null terminator
		char expected_prefix[expected_prefix_len];
		snprintf(expected_prefix, expected_prefix_len, "sofia/internal/%s", prefix);
		if (strncmp(channel_name, expected_prefix, expected_prefix_len - 1) != 0) {
			// 忽略其余channel
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Ignoring '%s' event: Channel-Name not filter: %s\n", event_name, channel_name);
			goto done;
		}
		const char *start_pos = channel_name + strlen("sofia/internal/");
		const char *end_pos = strchr(start_pos, '@');
		if (end_pos != NULL) {
            // 计算需要提取的子字符串长度
			size_t extracted_len = end_pos - start_pos;
			char extracted_part[extracted_len + 1];
			strncpy(extracted_part, start_pos, extracted_len);
			extracted_part[extracted_len] = '\0';  // 确保字符串以null结尾
			snprintf(msg.key, ROCKETMQ_MAX_KEY_LENGTH, "%s", extracted_part);
		}
	} else {
		// Default handling for other events
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

	// Convert event to JSON string
	if (switch_event_serialize_json(evt, &json_str) == SWITCH_STATUS_SUCCESS) {
		msg.pjson = json_str;
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Generated JSON: %s\n", msg.pjson);
		if (strlen(msg.pjson) > mod_rocketmq_globals.active_profile->max_message_size) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Dropping event: message size %zu exceeds max_message_size %u\n",
							strlen(msg.pjson), mod_rocketmq_globals.active_profile->max_message_size);
			goto done;
		}

		unsigned int send_attempt = 0;
        switch_bool_t success = SWITCH_FALSE;
        unsigned int reconnect_retry_times = mod_rocketmq_globals.active_profile->reconnect_retry_times;
        unsigned int reconnect_retry_interval = mod_rocketmq_globals.active_profile->reconnect_retry_interval;

        while (!success && send_attempt <= reconnect_retry_times) {
            try {
				// Create RocketMQ message
				rocketmq::MQMessage rocketmq_msg(msg.topic, msg.tag, msg.pjson);
				if (strlen(msg.key) > 0) {
					rocketmq_msg.setKeys(msg.key);
				}

                // Send message - ensure same key goes to same queue for ordering
                rocketmq::SendResult send_result;
                
                // For ordered messages, use key-based routing to ensure same key goes to same queue
				if (strcmp(mod_rocketmq_globals.active_profile->message_mode, "ordered") == 0) {
					// Calculate queue ID based on key hash
					uint32_t hash = 0;
					for (const char *p = msg.key; *p; p++) {
						hash = hash * 31 + *p;
					}
                    
                    // Use configured queue count to calculate queue ID
                    int queue_count = mod_rocketmq_globals.active_profile->topic_queue_count;
					int queue_id = hash % queue_count;
					
					// Set key to ensure same key goes to same queue
					rocketmq_msg.setKeys(msg.key);
                    
                    // Define a simple queue selector that uses the calculated queue_id
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
                    
                    // Send message using the custom selector and calculated queue_id
					send_result = mod_rocketmq_globals.producer->send(rocketmq_msg, &selector, &queue_id);
					
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Sent ordered message to RocketMQ: topic=%s, tag=%s, key=%s, calculatedQueueId=%d, msgId=%s\n", 
									msg.topic, msg.tag, msg.key, queue_id, send_result.getMsgId().c_str());
				} else {
					// Use normal send for unordered messages
					send_result = mod_rocketmq_globals.producer->send(rocketmq_msg);
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Sent unordered message to RocketMQ: topic=%s, tag=%s, key=%s, msgId=%s\n", 
									msg.topic, msg.tag, msg.key, send_result.getMsgId().c_str());
				}
				success = SWITCH_TRUE;
			} catch (const std::exception& e) {
				send_attempt++;
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to send message to RocketMQ (attempt %u/%u): Topic=%s, Tag=%s, Key=%s, Error: %s\n", 
								send_attempt, reconnect_retry_times, msg.topic, msg.tag, msg.key, e.what());
                
                // Check if the error is about route info
                std::string error_str(e.what());
                bool is_route_error = (error_str.find("No route info for this topic") != std::string::npos);
                
				if (is_route_error) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Route error detected for topic: %s\n", msg.topic);
				}
                
                // If we still have retries left, restart producer and try again
                if (send_attempt <= reconnect_retry_times) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Restarting RocketMQ producer and retrying...\n");
                    
                    // Restart producer to refresh route info
                    if (mod_rocketmq_restart_producer()) {
                        // Wait before retrying
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Waiting %u ms before retry...\n", reconnect_retry_interval);
                        switch_yield(reconnect_retry_interval * 1000); // Convert ms to microseconds
                    } else {
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to restart producer, giving up\n");
                        break;
                    }
                } else {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Max retry attempts reached, giving up\n");
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Last error: %s\n", e.what());
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
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
}

/* ------------------------------
   Configuration functions
   ------------------------------
*/
switch_status_t mod_rocketmq_do_config(switch_bool_t reload)
{
	switch_xml_t cfg, xml, profiles_node, profile_node, param_node;
	switch_status_t status = SWITCH_STATUS_SUCCESS;
	
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Loading RocketMQ configuration...\n");
	
	if (reload) {
		/* Unbind all event handlers if reloading */
		switch_event_unbind_callback(mod_rocketmq_event_handler);
	}

	switch_mutex_lock(mod_rocketmq_globals.mutex);

	if (reload) {
		/* Destroy old profiles */
		mod_rocketmq_destroy_old_profiles();
	}
    
    /* Open configuration file */
	xml = switch_xml_open_cfg("rocketmq.conf", &cfg, NULL);
	if (!xml || !cfg) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to open rocketmq.conf.xml, using default settings\n");
		/* Return success even if config file is missing */
		goto done;
	}
    
    /* Get profiles node */
	profiles_node = switch_xml_child(cfg, "profiles");
	if (!profiles_node) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "No profiles node found in rocketmq.conf.xml, using default settings\n");
		switch_xml_free(xml);
		goto done;
	}
    
    /* Iterate through profiles */
    for (profile_node = switch_xml_child(profiles_node, "profile"); profile_node; profile_node = profile_node->next) {
        const char *name = switch_xml_attr_soft(profile_node, "name");
        
		if (name) {
			mod_rocketmq_profile_t *profile = NULL;
			switch_bool_t has_send_timeout = SWITCH_FALSE;
			switch_bool_t has_retry_times = SWITCH_FALSE;
			switch_bool_t has_max_message_size = SWITCH_FALSE;
			switch_bool_t has_reconnect_retry_times = SWITCH_FALSE;
			switch_bool_t has_reconnect_retry_interval = SWITCH_FALSE;
			switch_bool_t has_topic_queue_count = SWITCH_FALSE;
			
			/* Create new profile */
			profile = (mod_rocketmq_profile_t *)switch_core_alloc(mod_rocketmq_globals.pool, sizeof(mod_rocketmq_profile_t));
            memset(profile, 0, sizeof(mod_rocketmq_profile_t));
            profile->name = switch_core_strdup(mod_rocketmq_globals.pool, name);
            
            /* Add to hash */
            switch_core_hash_insert(mod_rocketmq_globals.profile_hash, profile->name, profile);
            
            /* Initialize default event subscription */
            profile->events[0].id = SWITCH_EVENT_ALL;
            profile->events[0].subclass = SWITCH_EVENT_SUBCLASS_ANY;
            profile->event_subscriptions = 1;
            
            /* Parse parameters */
            for (param_node = switch_xml_child(profile_node, "param"); param_node; param_node = param_node->next) {
                const char *param_name = switch_xml_attr_soft(param_node, "name");
                const char *param_value = switch_xml_attr_soft(param_node, "value");
                
                if (param_name && param_value) {
                    if (!strcasecmp(param_name, "namesrv")) {
                        profile->namesrv = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "group_id")) {
                        profile->group_id = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "access_key")) {
                        profile->access_key = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "secret_key")) {
                        profile->secret_key = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "topic")) {
                        profile->topic = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "tag")) {
                        profile->tag = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
                    } else if (!strcasecmp(param_name, "filter_account_prefix")) {
                        profile->filter_account_prefix = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
					} else if (!strcasecmp(param_name, "event_filter")) {
						char *dup_tmp = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
						char *argv[SWITCH_EVENT_ALL];
						int arg, token_count, valid_count = 0;
						
						/* Parse event filter */
						token_count = switch_separate_string(dup_tmp, ',', argv, (sizeof(argv) / sizeof(argv[0])));
						
						switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Found %d raw event subscriptions\n", token_count);
						
						for (arg = 0; arg < token_count; arg++) {
							char *subclass = (char *)SWITCH_EVENT_SUBCLASS_ANY;
							char *event_copy = switch_core_strdup(mod_rocketmq_globals.pool, argv[arg]);
							char *trimmed_event = event_copy;
							char *end;

							/* Trim event name */
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

								/* Trim event again after split */
								end = trimmed_event + strlen(trimmed_event) - 1;
								while (end > trimmed_event && isspace((unsigned char)*end)) {
									end--;
								}
								*(end + 1) = '\0';

								/* Trim subclass */
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
								switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Added event subscription: %s with subclass %s\n", 
												trimmed_event, subclass == SWITCH_EVENT_SUBCLASS_ANY ? "ANY" : subclass);
							} else {
								switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "The switch event %s was not recognised.\n", trimmed_event);
							}
						}

						if (valid_count > 0) {
							profile->event_subscriptions = valid_count;
						} else {
							switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "No valid event_filter entries found, falling back to SWITCH_EVENT_ALL\n");
							profile->events[0].id = SWITCH_EVENT_ALL;
							profile->events[0].subclass = SWITCH_EVENT_SUBCLASS_ANY;
							profile->event_subscriptions = 1;
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
						profile->message_mode = switch_core_strdup(mod_rocketmq_globals.pool, param_value);
					} else if (!strcasecmp(param_name, "topic_queue_count")) {
						unsigned int value = 0;
						if (mod_rocketmq_parse_uint_param(param_name, param_value, 1, 65535, &value)) {
							profile->topic_queue_count = value;
							has_topic_queue_count = SWITCH_TRUE;
						}
					}
				}
			}
            
            /* Set default values */
            if (!profile->namesrv) {
                profile->namesrv = switch_core_strdup(mod_rocketmq_globals.pool, "localhost:9876");
            }
            if (!profile->group_id) {
                profile->group_id = switch_core_strdup(mod_rocketmq_globals.pool, "freeswitch_producer_group");
            }
            if (!profile->topic) {
                profile->topic = switch_core_strdup(mod_rocketmq_globals.pool, "freeswitch_events");
            }
            if (!profile->tag) {
                profile->tag = switch_core_strdup(mod_rocketmq_globals.pool, "event");
            }
            if (!profile->filter_account_prefix) {
                profile->filter_account_prefix = switch_core_strdup(mod_rocketmq_globals.pool, "BYTEL"); /* Default prefix */
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
				profile->message_mode = switch_core_strdup(mod_rocketmq_globals.pool, "ordered");
			}
			if (!has_topic_queue_count) {
				profile->topic_queue_count = 4; /* Default to 4 queues per topic */
			}
            
            /* Set as active profile if none is active */
            if (!mod_rocketmq_globals.active_profile) {
                mod_rocketmq_globals.active_profile = profile;
            }
        }
    }
    
    /* Free XML */
    switch_xml_free(xml);
    
    /* Print current configuration */
    if (mod_rocketmq_globals.active_profile) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Current RocketMQ configuration:\n");
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Active profile: %s\n", mod_rocketmq_globals.active_profile->name);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Namesrv: %s\n", mod_rocketmq_globals.active_profile->namesrv);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Group ID: %s\n", mod_rocketmq_globals.active_profile->group_id);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Access Key: %s\n", mod_rocketmq_globals.active_profile->access_key ? "set" : "not set");
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Secret Key: %s\n", mod_rocketmq_globals.active_profile->secret_key ? "set" : "not set");
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Topic: %s\n", mod_rocketmq_globals.active_profile->topic);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Tag: %s\n", mod_rocketmq_globals.active_profile->tag);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Filter Account Prefix: %s\n", mod_rocketmq_globals.active_profile->filter_account_prefix);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Send timeout: %u ms\n", mod_rocketmq_globals.active_profile->send_timeout);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Retry times: %u\n", mod_rocketmq_globals.active_profile->retry_times);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Max message size: %u bytes\n", mod_rocketmq_globals.active_profile->max_message_size);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Reconnect retry times: %u\n", mod_rocketmq_globals.active_profile->reconnect_retry_times);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Reconnect retry interval: %u ms\n", mod_rocketmq_globals.active_profile->reconnect_retry_interval);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Topic queue count: %u\n", mod_rocketmq_globals.active_profile->topic_queue_count);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Message mode: %s\n", mod_rocketmq_globals.active_profile->message_mode);
        
        /* Print event subscriptions */
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "  Event subscriptions (%d):\n", mod_rocketmq_globals.active_profile->event_subscriptions);
        for (int i = 0; i < mod_rocketmq_globals.active_profile->event_subscriptions; i++) {
            const char *event_name = switch_event_name(mod_rocketmq_globals.active_profile->events[i].id);
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "    %d: %s%s%s\n", 
                            i+1, 
                            event_name ? event_name : "UNKNOWN", 
                            mod_rocketmq_globals.active_profile->events[i].subclass && mod_rocketmq_globals.active_profile->events[i].subclass != SWITCH_EVENT_SUBCLASS_ANY ? "^" : "", 
                            mod_rocketmq_globals.active_profile->events[i].subclass && mod_rocketmq_globals.active_profile->events[i].subclass != SWITCH_EVENT_SUBCLASS_ANY ? mod_rocketmq_globals.active_profile->events[i].subclass : "");
        }
        
        /* Re-bind events if reloading */
        if (reload) {
            int i;
            for (i = 0; i < mod_rocketmq_globals.active_profile->event_subscriptions; i++) {
                if (switch_event_bind_removable("ROCKETMQ",
                                                mod_rocketmq_globals.active_profile->events[i].id,
                                                mod_rocketmq_globals.active_profile->events[i].subclass,
                                                mod_rocketmq_event_handler,
                                                NULL,
                                                &(mod_rocketmq_globals.active_profile->event_nodes[i])) != SWITCH_STATUS_SUCCESS) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind to event handler %d!", (int)mod_rocketmq_globals.active_profile->events[i].id);
                }
            }
        }
    }
    
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ configuration loaded successfully\n");

done:
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
	return status;
}

/* ------------------------------
   Startup
   ------------------------------
*/
SWITCH_MODULE_LOAD_FUNCTION(mod_rocketmq_load)
{
    switch_api_interface_t *api_interface;
    int i;
    bool bind_failed = false;
    
    /* Initialize globals */
    memset(&mod_rocketmq_globals, 0, sizeof(mod_rocketmq_globals_t));
    *module_interface = switch_loadable_module_create_module_interface(pool, modname);
    
	mod_rocketmq_globals.pool = pool;
	switch_core_hash_init(&(mod_rocketmq_globals.profile_hash));
	switch_mutex_init(&(mod_rocketmq_globals.mutex), SWITCH_MUTEX_NESTED, pool);
    
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq loading: Version %s\n", switch_version_full());
    
    /* Load configuration - continue even if it fails */
    if (mod_rocketmq_do_config(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Failed to load configuration, using default settings\n");
    }
    
    /* Initialize RocketMQ producer if we have an active profile */
    if (mod_rocketmq_globals.active_profile) {
        try {
            /* Create producer */
            mod_rocketmq_globals.producer = new rocketmq::DefaultMQProducer(mod_rocketmq_globals.active_profile->group_id);
            
            /* Set producer properties */
            mod_rocketmq_globals.producer->setNamesrvAddr(mod_rocketmq_globals.active_profile->namesrv);
            mod_rocketmq_globals.producer->setSendMsgTimeout(mod_rocketmq_globals.active_profile->send_timeout);
            mod_rocketmq_globals.producer->setRetryTimes(mod_rocketmq_globals.active_profile->retry_times);
            
            /* Set authentication if access_key and secret_key are provided */
            if (mod_rocketmq_globals.active_profile->access_key && mod_rocketmq_globals.active_profile->secret_key &&
                strlen(mod_rocketmq_globals.active_profile->access_key) > 0 && strlen(mod_rocketmq_globals.active_profile->secret_key) > 0) {
                mod_rocketmq_globals.producer->setSessionCredentials(mod_rocketmq_globals.active_profile->access_key,
                                                                mod_rocketmq_globals.active_profile->secret_key,
                                                                "");
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication enabled\n");
            } else {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer authentication disabled\n");
            }
            
            /* Start producer */
            mod_rocketmq_globals.producer->start();
            
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer started successfully\n");
            
            /* Validate configured topic */
            if (mod_rocketmq_globals.active_profile->topic) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Validating configured topic: %s\n", 
                                mod_rocketmq_globals.active_profile->topic);
                
                // Try to validate the topic
                if (!mod_rocketmq_validate_topic(mod_rocketmq_globals.active_profile->topic)) {
                    // If validation fails, try to refresh route
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Trying to refresh topic route for: %s\n", 
                                    mod_rocketmq_globals.active_profile->topic);
                    
                    mod_rocketmq_refresh_topic_route(mod_rocketmq_globals.active_profile->topic);
                    
                    // Validate again after refresh
                    if (mod_rocketmq_validate_topic(mod_rocketmq_globals.active_profile->topic)) {
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Topic validation successful after refresh: %s\n", 
                                        mod_rocketmq_globals.active_profile->topic);
                    } else {
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Topic validation failed: %s\n", 
                                        mod_rocketmq_globals.active_profile->topic);
                        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "No route info of this topic. Please check if the topic exists in RocketMQ.\n");
                        
                        // Clean up producer before returning error
                    if (mod_rocketmq_globals.producer) {
                        try {
                            mod_rocketmq_globals.producer->shutdown();
                        } catch (const std::exception &e) {
                            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error during producer shutdown: %s\n", e.what());
                        }
                        delete mod_rocketmq_globals.producer;
                        mod_rocketmq_globals.producer = NULL;
                    }
                        
                        return SWITCH_STATUS_GENERR;
                    }
                }
            }
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to initialize RocketMQ producer: %s\n", e.what());
            /* Clean up on failure */
            if (mod_rocketmq_globals.producer) {
                delete mod_rocketmq_globals.producer;
                mod_rocketmq_globals.producer = NULL;
            }
            return SWITCH_STATUS_GENERR;
        }
    } else {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "No active profile, RocketMQ producer will not be initialized\n");
        return SWITCH_STATUS_GENERR;
    }
    
    /* Bind event handlers based on configuration */
    if (mod_rocketmq_globals.active_profile) {
        for (i = 0; i < mod_rocketmq_globals.active_profile->event_subscriptions; i++) {
            if (mod_rocketmq_globals.active_profile->events[i].subclass && mod_rocketmq_globals.active_profile->events[i].subclass != SWITCH_EVENT_SUBCLASS_ANY) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Binding to event %d with subclass %s", 
                                (int)mod_rocketmq_globals.active_profile->events[i].id, 
                                mod_rocketmq_globals.active_profile->events[i].subclass);
            } else {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Binding to event %d with any subclass", 
                                (int)mod_rocketmq_globals.active_profile->events[i].id);
            }
            
            if (switch_event_bind_removable("ROCKETMQ",
                                            mod_rocketmq_globals.active_profile->events[i].id,
                                            mod_rocketmq_globals.active_profile->events[i].subclass,
                                            mod_rocketmq_event_handler,
                                            NULL,
                                            &(mod_rocketmq_globals.active_profile->event_nodes[i])) != SWITCH_STATUS_SUCCESS) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind to event handler %d!", (int)mod_rocketmq_globals.active_profile->events[i].id);
                bind_failed = true;
            }
        }
        
        // If any bind failed, clean up and return error
        if (bind_failed) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to bind one or more event handlers, cleaning up...");
            
            // Unbind all event handlers that were successfully bound
                for (i = 0; i < mod_rocketmq_globals.active_profile->event_subscriptions; i++) {
                    if (mod_rocketmq_globals.active_profile->event_nodes[i]) {
                        switch_event_unbind(&mod_rocketmq_globals.active_profile->event_nodes[i]);
                    }
                }
            
            // Clean up producer
            if (mod_rocketmq_globals.producer) {
                try {
                    mod_rocketmq_globals.producer->shutdown();
                } catch (const std::exception &e) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error during producer shutdown: %s", e.what());
                }
                delete mod_rocketmq_globals.producer;
                mod_rocketmq_globals.producer = NULL;
            }
            
            return SWITCH_STATUS_GENERR;
        }
    } else {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "No active profile, binding to all events as fallback");
        
        /* No active profile, bind to all events as fallback */
        if (switch_event_bind_removable("ROCKETMQ",
                                        SWITCH_EVENT_ALL,
                                        SWITCH_EVENT_SUBCLASS_ANY,
                                        mod_rocketmq_event_handler,
                                        NULL,
                                        NULL) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind to event handler!");
            return SWITCH_STATUS_GENERR;
        }
    }
    
    /* Add API */
    SWITCH_ADD_API(api_interface, "rocketmq", "rocketmq API", rocketmq_reload, "syntax");
    
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq loaded successfully\n");
    return SWITCH_STATUS_SUCCESS;
}

/* ------------------------------
   Shutdown
   ------------------------------
*/
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_rocketmq_shutdown)
{
	/* Unbind event handler */
	switch_event_unbind_callback(mod_rocketmq_event_handler);

	switch_mutex_lock(mod_rocketmq_globals.mutex);
	
	/* Shutdown producer */
	if (mod_rocketmq_globals.producer) {
        try {
            mod_rocketmq_globals.producer->shutdown();
            delete mod_rocketmq_globals.producer;
            mod_rocketmq_globals.producer = NULL;
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "RocketMQ producer shutdown successfully\n");
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to shutdown producer: %s\n", e.what());
            /* Ensure producer is deleted even if shutdown fails */
            delete mod_rocketmq_globals.producer;
            mod_rocketmq_globals.producer = NULL;
        }
    }
    
    /* Destroy profile hash */
	if (mod_rocketmq_globals.profile_hash) {
		switch_core_hash_destroy(&(mod_rocketmq_globals.profile_hash));
	}

	mod_rocketmq_globals.active_profile = NULL;
	switch_mutex_unlock(mod_rocketmq_globals.mutex);
    
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_rocketmq shutdown complete\n");
    return SWITCH_STATUS_SUCCESS;
}
