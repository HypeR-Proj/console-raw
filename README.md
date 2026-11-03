<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# console-raw

A HypeR component. See [HypeR](https://github.com/HypeR-Proj/HypeR) for
workspace setup and the installed SDK build boundary.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test console-raw
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Admission failures

Control, log and stream listeners discard matched transfer rejections and malformed
peer requests. Stream protocol refusals use `InvalidStreamRequest`; trusted SDK
`InvalidResponse` failures are diagnosed and stop the affected listener. Finite
wait timeouts allow generation/stop checks. Existing sessions keep their owners.
