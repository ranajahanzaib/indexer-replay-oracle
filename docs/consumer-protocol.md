# Consumer subprocess protocol v1

Transport: UTF-8 JSON objects terminated by LF over stdin/stdout. A request receives exactly one response carrying its integer `request_id`, except the negotiated crash hook. The runner keeps one request in flight. IDs start at 0 for hello and increase within each process; restarts begin a new session. Diagnostics must go to stderr. No shell is used to launch the executable.

After idempotent migrations and connecting to its isolated database, respond to:

```json
{ "type": "hello", "request_id": 0, "version": 1 }
```

```json
{
  "type": "hello",
  "request_id": 0,
  "version": 1,
  "capabilities": ["durable_ack", "shutdown", "fault.after_commit_before_ack"],
  "schema": null
}
```

Version 1 and `durable_ack` are mandatory. PostgreSQL additionally requires `schema_isolation` and `schema` equal to `IRO_DATABASE_SCHEMA`; reply only after verifying `current_schema()` on the actual connection. SQLite uses null. The harness selects only from its qualified schema. A dishonest consumer that falsely attests isolation violates the trusted protocol contract.

Apply request:

```text
{"type":"apply","request_id":1,"event":<complete canonical event>,"fault":null}
```

Successful durable commit:

```json
{ "type": "ack", "request_id": 1 }
```

Handler error (the harness records the code, avoiding arbitrary secret-bearing error text):

```json
{
  "type": "error",
  "request_id": 1,
  "code": "INVALID_EVENT",
  "message": "invalid amount"
}
```

On `fault:"after_commit_before_ack"`, a capable consumer must apply/commit the event and exit with status **86**, emitting no ack. The runner detects EOF, checks process exit and code, relaunches on the same database, redelivers the event with no fault, and requires an ack. No random kill is claimed to prove a commit boundary. If the capability is absent the scenario is unsupported, overall exit 2 when enabled. If the hook returns an ack or exits with another code the scenario fails. A duplicate already committed event must also honor the requested exit hook after its idempotent transaction.

Graceful shutdown when advertised:

```json
{ "type": "shutdown", "request_id": 2 }
```

Reply `ack` and exit successfully. Without `shutdown`, the runner kills and reaps the child when finished. Unexpected exit, mismatched IDs/versions, invalid response kinds, malformed JSON, excessive output, and timeouts fail explicitly. Child handles use kill-on-drop; stderr is drained continuously with bounded capture to avoid deadlock. Raw stderr is not persisted because it may contain credentials. Protocol error messages intentionally avoid echoing arbitrary consumer payloads. Consumers must not spawn detached children; the harness controls its direct child, not an OS process sandbox.

Runtime environment:

- `IRO_DATABASE_URL`: new temporary SQLite URL or local PostgreSQL server URL.
- `IRO_DATABASE_SCHEMA`: only PostgreSQL; must be used on every consumer connection.
- `IRO_RUN_SEED`: decimal schedule seed.
- `IRO_RUN_ID`: scenario label, not an event identity.

Other environment variables are inherited. Manifests and consumer code are trusted configuration. Runtime database URLs are never added to reports or bundles. Do not put secrets into command arguments, fixture payloads, or consumer error codes: these are intentionally reportable test data.
