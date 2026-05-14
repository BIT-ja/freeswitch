mod_rocketmq
============

Overview
--------

`mod_rocketmq` publishes FreeSWITCH events to RocketMQ.
Events are serialized to JSON and sent using topic/tag/key from the active profile and event headers.

Current implementation characteristics:

- Single global producer instance.
- Single active profile (the first valid `<profile>` in config).
- Event callback does not hold global mutex during network send/retry/sleep.
- Reload uses parse-then-swap with rollback and producer rebuild.


Build and Install
-----------------

This module is guarded by `HAVE_ROCKETMQ` in [Makefile.am](./Makefile.am).
RocketMQ C++ client headers and libraries are required at build time.

Typical flow:

1. Enable `event_handlers/mod_rocketmq` in `modules.conf`.
2. Run normal FreeSWITCH build steps (`bootstrap.sh`, `configure`, `make`).
3. Install and load `mod_rocketmq.so`.

Module-only build commands:

- `make -C src/mod mod_rocketmq-all`
- `make -C src/mod mod_rocketmq-install`

Packaged RocketMQ client artifacts are included in this directory:

- `rocketmq-client-cpp-2.2.0.amd64.deb`
- `rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm`


Configuration
-------------

The module loads `rocketmq.conf` (normally `autoload_configs/rocketmq.conf.xml`).
Example:

```xml
<configuration name="rocketmq.conf" description="mod_rocketmq">
  <profiles>
    <profile name="default">
      <param name="namesrv" value="localhost:9876"/>
      <param name="group_id" value="freeswitch_producer_group"/>
      <param name="access_key" value=""/>
      <param name="secret_key" value=""/>
      <param name="topic" value="freeswitch_events"/>
      <param name="tag" value="event"/>
      <param name="filter_account_prefix" value="BYTEL"/>
      <param name="send_timeout" value="3000"/>
      <param name="retry_times" value="3"/>
      <param name="max_message_size" value="4096"/>
      <param name="reconnect_retry_times" value="5"/>
      <param name="reconnect_retry_interval" value="1000"/>
      <param name="topic_queue_count" value="4"/>
      <param name="message_mode" value="ordered"/>
      <param name="topic_validation_mode" value="none"/>
      <param name="event_filter" value="CHANNEL_CREATE,CHANNEL_ANSWER,API,CUSTOM^sofia::register"/>
    </profile>
  </profiles>
</configuration>
```

Supported profile parameters:

- `namesrv` (default: `localhost:9876`)
- `group_id` (default: `freeswitch_producer_group`)
- `access_key` (optional)
- `secret_key` (optional)
- `topic` (default: `freeswitch_events`)
- `tag` (default: `event`)
- `filter_account_prefix` (default: `BYTEL`)
- `event_filter` (comma-separated, supports `EVENT` or `EVENT^subclass`)
- `send_timeout` in ms (range: `1..600000`, default: `3000`)
- `retry_times` (range: `0..100`, default: `3`)
- `max_message_size` in bytes (range: `1..4194304`, default: `4096`)
- `reconnect_retry_times` (range: `0..1000`, default: `5`)
- `reconnect_retry_interval` in ms (range: `1..600000`, default: `1000`)
- `topic_queue_count` (range: `1..65535`, default: `4`)
- `message_mode` (`ordered` or `unordered`, default: `ordered`)
- `topic_validation_mode` (`none` or `send_test_message`, default: `none`)


Runtime Behavior
----------------

Load sequence:

1. Parse config and choose active profile.
2. Create/start producer.
3. Optionally validate topic when `topic_validation_mode=send_test_message`.
4. Bind configured events.

Event handler behavior:

- Skip event when producer/profile is unavailable.
- Serialize event with `switch_event_serialize_json`.
- Drop oversized payload (`max_message_size`).
- Build message key by event type:
  - `CUSTOM`: uses `sip_auth_username`; optional prefix check via `filter_account_prefix`.
  - `CHANNEL_*`: requires `Channel-Name` prefix `sofia/internal/<filter_account_prefix>`, then extracts substring before `@`.
  - Others: fallback to `Event-Name + Core-UUID + Event-Sequence` (or timestamp).
- On send error: retry up to `reconnect_retry_times`; each retry restarts producer and waits `reconnect_retry_interval`.

Ordered mode:

- If `message_mode=ordered`, queue selection is key-hash based.
- Queue id is derived from `hash(key) % topic_queue_count`, then mapped to the broker queue list.

Concurrency model:

- Event send acquires a short-lived producer reference, then releases it after each attempt.
- Producer restart/shutdown waits for in-flight sends to finish before swapping/deleting producer.


CLI/API
-------

The module registers:

- `api rocketmq`

Current behavior:

- Parses and validates new config first.
- Rebinds events and rebuilds producer on success.
- Rolls back to previous config/event bindings when reload fails.

Risk Remediation Plan and Status
--------------------------------

1. Reduce lock scope in event callback: done.
2. Prevent reload from silently dropping forwarding: done.
3. Avoid reload-time config memory accumulation: done.
4. Ensure reload applies connection-level producer params: done.
5. Remove default side-effect topic validation on startup: done (`topic_validation_mode=none`).
6. Remove hardcoded ABI macro and non-portable VLA usage: done.


Remaining Improvement Backlog
-----------------------------

1. Add async internal queue + worker thread to decouple callback from broker RTT.
2. Add metrics (`success/fail/retry/restart/drop`) for observability.
3. Add regression tests for reload rollback, key routing, and filter parsing.


Operational Recommendations
---------------------------

- For connection-level changes (`namesrv`, auth, timeout), `api rocketmq` now applies via producer rebuild; module unload/load is optional fallback.
- Keep `event_filter` narrow in production to avoid unnecessary serialization/sending overhead.
- Increase `max_message_size` only after checking RocketMQ broker/client limits and network impact.
- Keep `topic_validation_mode=none` in production unless you explicitly need startup send-test validation.
