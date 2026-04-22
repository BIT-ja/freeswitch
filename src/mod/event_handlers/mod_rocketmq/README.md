mod_rocketmq
============

Overview
--------

`mod_rocketmq` publishes selected FreeSWITCH events to RocketMQ.
Each event is serialized to JSON and sent with a topic/tag/key derived from profile settings and event headers.

The module currently uses:

- One global producer instance.
- One active profile (the first `<profile>` in `rocketmq.conf.xml`).


Build and Install
-----------------

This module is guarded by `HAVE_ROCKETMQ` in `Makefile.am` and requires RocketMQ C++ client headers/libs at build time.
`./configure` probes RocketMQ with the default C++ ABI first, then retries with `_GLIBCXX_USE_CXX11_ABI=0` for compatibility with old-ABI RocketMQ binaries.
The module build uses the ABI flags detected by `configure`.

Typical project flow:

1. Enable `event_handlers/mod_rocketmq` in `modules.conf`.
2. Run the normal FreeSWITCH build flow (`bootstrap.sh`, `configure`, `make`).
3. Ensure `mod_rocketmq.so` is installed in your module directory.

Build command behavior:

- `make mod_rocketmq` (from FreeSWITCH top-level) first builds `core` by design, then builds `src/mod/event_handlers/mod_rocketmq`.
- If you only want to build this module without triggering `core`, run:
  - `make -C src/mod mod_rocketmq-all`
  - `make -C src/mod mod_rocketmq-install`
- In repository GitHub Actions `macos` workflow, `mod_rocketmq` is explicitly commented out before `./configure` to avoid RocketMQ dependency failure on macOS runners.

There is also a local manual `Makefile` in this directory for standalone builds.

Configure-time dependency check:

- `./configure` now checks for RocketMQ C++ headers and `-lrocketmq`.
- If `mod_rocketmq` is enabled but dependency is missing, configure fails with an OS-aware package hint:
  - Debian/Ubuntu: `rocketmq-client-cpp-2.2.0.amd64.deb`
  - RHEL/CentOS family: `rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm`
- If RocketMQ is already installed but `configure` still reports `no`, rerun `bootstrap.sh` to regenerate autotools files, then run `./configure --with-rocketmq=/usr/local`.
- CI auto-install behavior:
  - `ci.sh` always tries to ensure RocketMQ dependency before running FreeSWITCH `./configure` (not gated by module comment state).
  - Debian/Ubuntu: install local `rocketmq-client-cpp-2.2.0.amd64.deb`
  - RHEL/CentOS-like: install local `rocketmq-client-cpp-2.2.0-centos7.x86_64.rpm`


Configuration
-------------

The module loads `rocketmq.conf` (usually `autoload_configs/rocketmq.conf.xml`).
This directory contains a sample `rocketmq.conf.xml`.

Example:

```xml
<configuration name="rocketmq.conf" description="mod_rocketmq">
  <profiles>
    <profile name="default">
      <param name="namesrv" value="127.0.0.1:9876"/>
      <param name="group_id" value="freeswitch_producer_group"/>
      <param name="topic" value="freeswitch_events"/>
      <param name="tag" value="event"/>
      <param name="filter_account_prefix" value="BYTEL"/>
      <param name="message_mode" value="ordered"/>
      <param name="topic_queue_count" value="4"/>
      <param name="reconnect_retry_times" value="5"/>
      <param name="reconnect_retry_interval" value="1000"/>
      <param name="event_filter" value="CHANNEL_CREATE,CHANNEL_ANSWER,API,CUSTOM^sofia::register"/>
    </profile>
  </profiles>
</configuration>
```

Supported profile parameters in current code path:

- `namesrv`
- `group_id`
- `access_key`
- `secret_key`
- `topic`
- `tag`
- `filter_account_prefix`
- `event_filter` (comma-separated, supports `EVENT` or `EVENT^subclass`)
- `send_timeout` (milliseconds)
- `retry_times`
- `max_message_size` (bytes)
- `reconnect_retry_times`
- `reconnect_retry_interval` (milliseconds)
- `message_mode` (`ordered` or other value for normal/unordered send)
- `topic_queue_count`


Runtime Behavior
----------------

Load sequence:

1. Parse config and select active profile.
2. Create/start RocketMQ producer.
3. Validate topic by sending a small test message.
4. Bind configured FreeSWITCH events.

Event handler behavior:

- If producer/profile is missing, events are skipped.
- Event is JSON-serialized with `switch_event_serialize_json`.
- Message larger than `max_message_size` is dropped.
- Message topic/tag come from active profile.
- Key generation:
  - `CUSTOM`: requires `sip_auth_username`; if `filter_account_prefix` is set, username must start with that prefix.
  - `CHANNEL_*`: requires `Channel-Name` and must start with `sofia/internal/<filter_account_prefix>`; key is extracted from channel name before `@`.
  - Others: key falls back to `Event-Name`, `Core-UUID`, and `Event-Sequence` (or timestamp fallback).
- Retry behavior:
  - On send failure, the module retries up to `reconnect_retry_times`.
  - Before retry, it restarts producer and waits `reconnect_retry_interval`.

Ordered mode:

- When `message_mode=ordered`, queue selection is key-hash based.
- `queue_id = hash(key) % topic_queue_count`.
- A custom selector maps that queue id to the broker queue list so the same key is consistently routed.


Implementation Notes (Refactor)
-------------------------------

Latest refactor keeps wire behavior unchanged and focuses on C++ maintainability:

- Introduced RAII-style mutex guard and JSON buffer lifecycle management to reduce `goto`-based cleanup paths.
- Extracted key generation / retry send / producer startup into dedicated helper functions to simplify `mod_rocketmq_event_handler`.
- Unified producer initialization path for `load` and `restart` to avoid duplicated configuration branches.
- Removed source-level hardcoded ABI macro and rely on configure-detected ABI flags.
- Refactored `mod_rocketmq_do_config` into smaller helpers (event filter parsing, parameter parsing, default injection, config logging, reload rebind).
- Refactored `mod_rocketmq_load` to use dedicated helpers for topic validation/recovery and event-binding cleanup.
- Normalized `message_mode` parsing (trim/lowercase/whitelist/fallback) to reduce invalid-config ambiguity.
- Unified event-binding logic for load/reload and return explicit failure when reload bind fails.
- Added Chinese in-code comments on core control-flow paths (locking, key routing, retry/restart behavior) for easier team maintenance.


CLI/API
-------

The module registers API command:

- `api rocketmq`

This reloads configuration and re-binds event subscriptions.


Operational Notes
-----------------

- Topic validation sends a test message at module load time.
- Authentication is enabled only when both `access_key` and `secret_key` are non-empty.
- For full producer re-initialization after config changes (for example `namesrv`, auth, timeouts), prefer `unload mod_rocketmq` then `load mod_rocketmq`.


Known Limitations in Current Implementation
-------------------------------------------

- Only the first profile is used as active profile for producer and event binding.
- `api rocketmq` reloads profile/event bindings but does not fully rebuild producer connection parameters; for connection-level changes prefer module reload (`unload/load`).
