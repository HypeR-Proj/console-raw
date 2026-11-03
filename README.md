<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# console-raw

Bounded byte transport for the physical console.

Owns raw input/output sessions, independent log readers and console reset epochs.
Session management and terminal line discipline live in their own services; console
control and log admission use separate capability endpoints.

See the [implementation entry](src/main.rs) and [component declaration](component.json)
for the public interfaces, build dependencies and delivered artifacts.

## Boundaries

Terminal editing policy is deliberately absent. Queue/session bounds protect shared
service memory, and malformed clients must not disable future admission.

## Build and test

Use this checkout inside the [repo-managed workspace](https://github.com/HypeR-Proj/HypeR).
From this repository directory:

```sh
make
make test
```

An absolute `OUT=/path/to/out` selects a shared output directory. The module
entry delegates to build/system, which prepares the installed SDK and declared
dependencies. Native binaries require HypeR; host module checks cover the tests
selected by component.json. A standalone clone does not contain the workspace’s
other source repositories or Native SDK.


## Admission failures

Control, log and stream listeners discard matched transfer rejections and malformed
peer requests. Stream protocol refusals use `InvalidStreamRequest`; trusted SDK
`InvalidResponse` failures are diagnosed and stop the affected listener. Finite
wait timeouts allow generation/stop checks. Existing sessions keep their owners.
