# EventStream RPC wire protocol — captured reference for greengrass-ipc

> **Source of truth for framing.** Values here are transcribed from AWS's own implementations
> (aws-crt-python `awscrt/eventstream/rpc.py`, the Greengrass Java Nucleus `eventstreamrpc` package,
> and the Greengrass IPC docs). Do NOT guess these — implement against this file, then verify with
> the mock nucleus / a real device.

## 1. Transport & environment

- The nucleus runs an EventStream-RPC server on a **Unix domain socket** (plain, no TLS).
- A component process receives two environment variables from the nucleus:
  - `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT` — the socket path to connect to.
  - `SVCUID` — the per-component auth token (used in the Connect handshake).
- (There is also `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH` in some setups; components use the
  `..._FOR_COMPONENT` one.)

## 2. Frame format (handled by aws-smithy-eventstream)

Each message on the wire is a single EventStream frame:

```
[ total_len:u32 ][ headers_len:u32 ][ prelude_crc:u32 ][ headers... ][ payload... ][ message_crc:u32 ]
```

- All integers big-endian. `prelude_crc` covers the first 8 bytes; `message_crc` covers everything
  before it. `aws-smithy-eventstream::frame` (`MessageFrameDecoder`, `read_message_from`,
  `write_message_to`) implements all of this — we never compute CRCs by hand.
- Headers are typed key/value pairs. `aws_smithy_types::event_stream::{Message, Header, HeaderValue}`
  model them. The header value types we use: `String` (for `:content-type`, `service-model-type`,
  `:version`, error headers), `Int32` (for `:message-type`, `:message-flags`), and `Int32`/`Int64`
  for `:stream-id` (see §5 note).

## 3. MessageType (header `:message-type`, Int32)

From aws-crt-python `MessageType(IntEnum)`:

| Name                | Value | Notes |
|---------------------|-------|-------|
| APPLICATION_MESSAGE | 0     | Only on streams (stream-id != 0). Operation request/response/events. |
| APPLICATION_ERROR   | 1     | Only on streams. A modeled service error. |
| PING                | 2     | Protocol message (stream-id 0). |
| PING_RESPONSE       | 3     | Protocol message (stream-id 0). |
| CONNECT             | 4     | Protocol message (stream-id 0). The client's first message. |
| CONNECT_ACK         | 5     | Protocol message (stream-id 0). Server's reply to CONNECT. |
| PROTOCOL_ERROR      | 6     | Protocol message (stream-id 0). |
| INTERNAL_ERROR      | 7     | Protocol message (stream-id 0). |

Rule: APPLICATION_MESSAGE / APPLICATION_ERROR are **only** valid on a stream (positive stream-id);
all other types are **only** valid as protocol messages (stream-id 0).

## 4. MessageFlags (header `:message-flags`, Int32; bit flags, XOR together)

From aws-crt-python `MessageFlag`:

| Name                | Value | Notes |
|---------------------|-------|-------|
| NONE                | 0x0   | |
| CONNECTION_ACCEPTED | 0x1   | Present on CONNECT_ACK when the connection is accepted. **Absent ⇒ rejected.** |
| TERMINATE_STREAM    | 0x2   | Closes the stream after this message. Valid on any message type. |

## 5. Well-known headers

| Header name          | Type   | Used on | Meaning |
|----------------------|--------|---------|---------|
| `:message-type`      | Int32  | all     | see §3 |
| `:message-flags`     | Int32  | all     | see §4 |
| `:stream-id`         | Int32  | all     | 0 = connection/protocol; positive = a specific operation stream |
| `:content-type`      | String | app msgs (and CONNECT/ACK payloads) | always `application/json` for Greengrass |
| `:version`           | String | CONNECT | protocol version header (Greengrass uses `"0.1.0"`) |
| `service-model-type` | String | app msgs | the shape name, e.g. `aws.greengrass#UpdateStateRequest`, or the event union for streams |
| `operation`          | String | first msg on a stream (activate) | the operation name, e.g. `aws.greengrass#UpdateState` |

Notes:
- Java Nucleus confirms `:content-type` = `"application/json"` and uses `CONTENT_TYPE_HEADER` /
  `SERVICE_MODEL_TYPE_HEADER` / `VERSION_HEADER` constants (values `:content-type`,
  `service-model-type`, `:version`).
- `:stream-id` is written as Int32 by the CRT. When reading, accept Int32/Int16 header value forms.
- The `operation` header is sent on the **first** message of a stream (the CRT "activate" call). The
  server's `ServiceOperationMappingContinuationHandler.onIncomingStream(continuation, operationName)`
  dispatches on it.

## 6. Connect handshake (the critical, non-framing part)

1. Client connects the socket.
2. Client sends a **CONNECT** protocol message (stream-id 0, message-type 4):
   - Headers: `:message-type=4`, `:message-flags=0`, `:stream-id=0`, `:version="0.1.0"`,
     `:content-type="application/json"`.
   - **Payload: JSON** `{"authToken": "<SVCUID>"}`.
     (Java: `IPCEventStreamService.ipcAuthenticationHandler` parses the payload into
     `GreengrassEventStreamConnectMessage` and reads `getAuthToken()`. The JSON key is `authToken`.)
3. Server replies **CONNECT_ACK** (stream-id 0, message-type 5):
   - If accepted, `:message-flags` includes `CONNECTION_ACCEPTED (0x1)`. If that flag is **absent**,
     the connection was **rejected** — the client must treat it as an auth failure and close.
4. After an accepted ConnectAck, the client issues operations on positive stream-ids.

## 7. Issuing an operation

### Request/response
1. Allocate a new **positive** `:stream-id` (monotonic per connection; the CRT increments).
2. Send an APPLICATION_MESSAGE (type 0) as the first message on that stream ("activate"):
   - Headers: `:message-type=0`, `:message-flags=0` (or `TERMINATE_STREAM` if you want to close after
     one shot — but for request/response the server closes), `:stream-id=<id>`,
     `:content-type="application/json"`, `operation="aws.greengrass#<Op>"`,
     `service-model-type="aws.greengrass#<Op>Request"`.
   - Payload: JSON of the request shape.
3. Server responds on the same stream-id with an APPLICATION_MESSAGE (type 0) carrying the response
   JSON and `service-model-type="aws.greengrass#<Op>Response"`, usually with `TERMINATE_STREAM` set.
   On error, an APPLICATION_ERROR (type 1) with a modeled error shape + `service-model-type` naming
   the error (e.g. `aws.greengrass#ServiceError` / `aws.greengrass#ResourceNotFoundError`).

### Subscription (streaming)
1. Same as above to open the stream and send the subscription request; the server's first response is
   the subscription ack (a normal response shape, NOT terminate-stream).
2. The server then sends further APPLICATION_MESSAGE frames on the same stream-id, each carrying an
   event shape (e.g. `service-model-type="aws.greengrass#ComponentUpdatePolicyEvents"`), until the
   stream is closed.
3. To unsubscribe, send a message with `TERMINATE_STREAM`; the server also closes the stream on its
   side with `TERMINATE_STREAM`.

## 8. Read-loop routing

- One background task owns the socket read half + a `MessageFrameDecoder`.
- For each decoded frame, read `:stream-id`:
  - `0` → connection-level (ConnectAck during handshake; PING/errors otherwise).
  - positive → look up the registered sink for that stream-id and deliver (oneshot for
    request/response; mpsc for subscriptions). If `TERMINATE_STREAM` is set, close the sink after
    delivering.
- Application errors (type 1) resolve the pending request with a typed `Error::Service`.

## 9. Payload shapes

- All payloads are JSON. Field names are **camelCase** exactly as in the Python SDK
  `greengrasscoreipc/model.py`. Blobs are base64-encoded strings. Enums are strings.
- Examples we rely on:
  - `UpdateStateRequest`: `{"state": "RUNNING"|"ERRORED"}` (enum `LifecycleState`).
  - `DeferComponentUpdateRequest`: `{"deploymentId": "<id>", "message": "<optional>", "recheckAfterMs": <int>}`.
  - `PreComponentUpdateEvent`: `{"deploymentId": "<id>", "isGgcRestarting": <bool>}`.
  - `ComponentUpdatePolicyEvents` (tagged union): `{"preUpdateEvent": {...}}` or `{"postUpdateEvent": {...}}`.

## 10. Primary sources

- aws-crt-python EventStream RPC (message types/flags): `awscrt/eventstream/rpc.py`
  (https://github.com/awslabs/aws-crt-python).
- Greengrass Java Nucleus `software.amazon.awssdk.eventstreamrpc` (handshake, headers, dispatch) and
  `com.aws.greengrass.ipc.IPCEventStreamService` (authToken parsing): local clone at
  `~/reps/du7/aws-greengrass-nucleus`.
- Greengrass IPC docs: https://docs.aws.amazon.com/greengrass/v2/developerguide/interprocess-communication.html
- Shapes: Python SDK `awsiot/greengrasscoreipc/model.py`
  (https://github.com/aws/aws-iot-device-sdk-python-v2), and the Java generated shapes under
  `software/amazon/awssdk/aws/greengrass/` in the nucleus clone.
- Framing crate: https://docs.rs/aws-smithy-eventstream/latest/aws_smithy_eventstream/frame/
