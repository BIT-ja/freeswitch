/*
* FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
* Copyright (C) 2005-2024, Anthony Minessale II <anthm@freeswitch.org>
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
* mod_rocketmq.h -- Sends FreeSWITCH events to a RocketMQ broker
*
*/

#ifndef MOD_ROCKETMQ_H
#define MOD_ROCKETMQ_H

#include <switch.h>

// RocketMQ headers must be outside of extern "C" block for C++ template support
#include <rocketmq/DefaultMQProducer.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef _MSC_VER
#include <strings.h>
#endif

#define MAX_LOG_MESSAGE_SIZE 1024
#define ROCKETMQ_MAX_NAMESRV 4
#define ROCKETMQ_MAX_TOPIC_LENGTH 255
#define ROCKETMQ_MAX_TAG_LENGTH 64
#define ROCKETMQ_MAX_KEY_LENGTH 128

typedef struct {
    char topic[ROCKETMQ_MAX_TOPIC_LENGTH];
    char tag[ROCKETMQ_MAX_TAG_LENGTH];
    char key[ROCKETMQ_MAX_KEY_LENGTH];
    char *pjson;
} mod_rocketmq_message_t;

typedef struct {
    switch_event_types_t id;
    const char *subclass;
} mod_rocketmq_event_subscription_t;

typedef struct {
    char *name;
    char *namesrv;
    char *group_id;
    char *access_key;
    char *secret_key;
    char *topic;
    char *tag;
    char *filter_account_prefix;
    unsigned int send_timeout; /* in milliseconds */
    unsigned int retry_times;
    unsigned int max_message_size; /* in bytes */
    unsigned int reconnect_retry_times;
    unsigned int reconnect_retry_interval; /* in milliseconds */
    unsigned int topic_queue_count; /* number of queues for topic */
    char *message_mode; /* ordered or unordered */
    mod_rocketmq_event_subscription_t events[SWITCH_EVENT_ALL];
    int event_subscriptions;
    switch_event_node_t *event_nodes[SWITCH_EVENT_ALL];
} mod_rocketmq_profile_t;

typedef struct mod_rocketmq_globals_s {
	switch_memory_pool_t *pool;
	switch_hash_t *profile_hash;
	rocketmq::DefaultMQProducer *producer;
	mod_rocketmq_profile_t *active_profile;
	switch_mutex_t *mutex;
} mod_rocketmq_globals_t;

extern mod_rocketmq_globals_t mod_rocketmq_globals;

/* Event handler */
void mod_rocketmq_event_handler(switch_event_t* evt);

/* Configuration */
switch_status_t mod_rocketmq_do_config(switch_bool_t reload);

/* Producer functions */
void * SWITCH_THREAD_FUNC mod_rocketmq_producer_thread(switch_thread_t *thread, void *data);

#ifdef __cplusplus
}
#endif

#endif /* MOD_ROCKETMQ_H */
