//! Building and parsing Greengrass EventStream RPC messages on top of
//! [`aws_smithy_types::event_stream::Message`].

use super::{flags, headers, MessageType};
use crate::error::{Error, Result};
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use aws_smithy_types::str_bytes::StrBytes;
use bytes::Bytes;

/// A parsed view of an EventStream RPC message: the RPC-level fields we care about, plus the raw
/// JSON payload.
#[derive(Debug, Clone)]
pub(crate) struct RpcMessage {
    pub(crate) message_type: MessageType,
    pub(crate) flags: i32,
    pub(crate) stream_id: i32,
    pub(crate) service_model_type: Option<String>,
    pub(crate) payload: Bytes,
}

impl RpcMessage {
    /// Whether the `TERMINATE_STREAM` flag is set.
    pub(crate) fn terminates_stream(&self) -> bool {
        self.flags & flags::TERMINATE_STREAM != 0
    }

    /// Whether the `CONNECTION_ACCEPTED` flag is set.
    pub(crate) fn connection_accepted(&self) -> bool {
        self.flags & flags::CONNECTION_ACCEPTED != 0
    }

    /// Builds the `Connect` protocol message carrying the auth token payload.
    pub(crate) fn connect(payload: Bytes) -> Message {
        Message::new(payload)
            .add_header(int32(headers::MESSAGE_TYPE, MessageType::Connect as i32))
            .add_header(int32(headers::MESSAGE_FLAGS, flags::NONE))
            .add_header(int32(headers::STREAM_ID, 0))
            .add_header(string(headers::VERSION, headers::PROTOCOL_VERSION))
            .add_header(string(headers::CONTENT_TYPE, headers::CONTENT_TYPE_JSON))
    }

    /// Builds the first application message of an operation stream ("activate").
    pub(crate) fn activate(
        stream_id: i32,
        operation: &str,
        service_model_type: &str,
        payload: Bytes,
    ) -> Message {
        Message::new(payload)
            .add_header(int32(
                headers::MESSAGE_TYPE,
                MessageType::ApplicationMessage as i32,
            ))
            .add_header(int32(headers::MESSAGE_FLAGS, flags::NONE))
            .add_header(int32(headers::STREAM_ID, stream_id))
            .add_header(string(headers::CONTENT_TYPE, headers::CONTENT_TYPE_JSON))
            .add_header(string(headers::OPERATION, operation))
            .add_header(string(headers::SERVICE_MODEL_TYPE, service_model_type))
    }

    /// Builds a stream-terminating message (used to unsubscribe / close an operation stream).
    pub(crate) fn terminate(stream_id: i32) -> Message {
        Message::new(Bytes::new())
            .add_header(int32(
                headers::MESSAGE_TYPE,
                MessageType::ApplicationMessage as i32,
            ))
            .add_header(int32(headers::MESSAGE_FLAGS, flags::TERMINATE_STREAM))
            .add_header(int32(headers::STREAM_ID, stream_id))
    }

    /// Builds a `PingResponse` for a received `Ping`, echoing its payload.
    ///
    /// The EventStream RPC spec pairs every `Ping` with a `PingResponse` carrying the same
    /// payload, so a server using pings for liveness sees us as alive.
    pub(crate) fn ping_response(payload: Bytes) -> Message {
        Message::new(payload)
            .add_header(int32(
                headers::MESSAGE_TYPE,
                MessageType::PingResponse as i32,
            ))
            .add_header(int32(headers::MESSAGE_FLAGS, flags::NONE))
            .add_header(int32(headers::STREAM_ID, 0))
    }

    /// Parses an [`RpcMessage`] out of a decoded [`Message`].
    pub(crate) fn parse(message: &Message) -> Result<Self> {
        let mut message_type = None;
        let mut msg_flags = flags::NONE;
        let mut stream_id = 0i32;
        let mut service_model_type = None;
        let int = |header: &Header| {
            header_i32(header.value()).ok_or_else(|| {
                Error::frame(format!(
                    "header {} is not a 32-bit integer: {:?}",
                    header.name().as_str(),
                    header.value()
                ))
            })
        };

        for header in message.headers() {
            match header.name().as_str() {
                headers::MESSAGE_TYPE => {
                    message_type = Some(int(header)?);
                }
                headers::MESSAGE_FLAGS => {
                    msg_flags = int(header)?;
                }
                headers::STREAM_ID => {
                    stream_id = int(header)?;
                }
                headers::SERVICE_MODEL_TYPE => {
                    if let HeaderValue::String(s) = header.value() {
                        service_model_type = Some(s.as_str().to_owned());
                    }
                }
                _ => {}
            }
        }
        let raw_type =
            message_type.ok_or_else(|| Error::frame("message missing :message-type header"))?;
        let message_type = MessageType::from_i32(raw_type)
            .ok_or_else(|| Error::frame(format!("unknown :message-type value {raw_type}")))?;

        Ok(Self {
            message_type,
            flags: msg_flags,
            stream_id,
            service_model_type,
            payload: message.payload().clone(),
        })
    }
}

fn int32(name: &str, value: i32) -> Header {
    Header::new(name.to_owned(), HeaderValue::Int32(value))
}

fn string(name: &str, value: &str) -> Header {
    Header::new(
        name.to_owned(),
        HeaderValue::String(StrBytes::copy_from_str(value)),
    )
}

/// Reads an integer header value, accepting the various signed integer header encodings the wire
/// may use for `:message-type` / `:message-flags` / `:stream-id`. `None` if it is not an integer or
/// does not fit in an `i32`.
fn header_i32(value: &HeaderValue) -> Option<i32> {
    match value {
        HeaderValue::Int32(v) => Some(*v),
        HeaderValue::Int16(v) => Some(i32::from(*v)),
        HeaderValue::Byte(v) => Some(i32::from(*v)),
        HeaderValue::Int64(v) => i32::try_from(*v).ok(),
        _ => None,
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Error text is irrelevant to these properties, and formatting it symbolically never finishes.
    fn no_format(_: std::fmt::Arguments<'_>) -> String {
        String::new()
    }

    fn any_int_header_value() -> HeaderValue {
        match kani::any::<u8>() % 5 {
            0 => HeaderValue::Byte(kani::any()),
            1 => HeaderValue::Int16(kani::any()),
            2 => HeaderValue::Int32(kani::any()),
            3 => HeaderValue::Int64(kani::any()),
            _ => HeaderValue::Bool(kani::any()),
        }
    }

    /// Whatever integer encodings the nucleus picks for the three RPC headers, `parse` either
    /// rejects the message or returns exactly the values on the wire. It never panics.
    #[kani::proof]
    #[kani::unwind(15)] // memcmp over the longest header name compared, `:message-flags` (14 bytes)
    #[kani::stub(std::fmt::format, no_format)]
    fn parse_returns_wire_values_or_an_error() {
        let message = Message::new(Bytes::new())
            .add_header(Header::new(headers::MESSAGE_TYPE, any_int_header_value()))
            .add_header(Header::new(headers::MESSAGE_FLAGS, any_int_header_value()))
            .add_header(Header::new(headers::STREAM_ID, any_int_header_value()));

        if let Ok(rpc) = RpcMessage::parse(&message) {
            let h = message.headers();
            assert!(header_i32(h[0].value()) == Some(rpc.message_type as i32));
            assert!(header_i32(h[1].value()) == Some(rpc.flags));
            assert!(header_i32(h[2].value()) == Some(rpc.stream_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eventstream::flags;

    #[test]
    fn connect_message_has_expected_headers() {
        let msg = RpcMessage::connect(Bytes::from_static(b"{\"authToken\":\"x\"}"));
        let parsed = RpcMessage::parse(&msg).unwrap();
        assert_eq!(parsed.message_type, MessageType::Connect);
        assert_eq!(parsed.stream_id, 0);
        assert_eq!(parsed.flags, flags::NONE);
    }

    #[test]
    fn activate_roundtrips_operation_and_stream_id() {
        let msg = RpcMessage::activate(
            7,
            "aws.greengrass#UpdateState",
            "aws.greengrass#UpdateStateRequest",
            Bytes::from_static(b"{}"),
        );
        let parsed = RpcMessage::parse(&msg).unwrap();
        assert_eq!(parsed.message_type, MessageType::ApplicationMessage);
        assert_eq!(parsed.stream_id, 7);
        assert_eq!(
            parsed.service_model_type.as_deref(),
            Some("aws.greengrass#UpdateStateRequest")
        );
    }

    #[test]
    fn ping_response_echoes_payload_on_stream_zero() {
        let message = RpcMessage::ping_response(Bytes::from_static(b"beat"));
        let parsed = RpcMessage::parse(&message).unwrap();

        assert_eq!(parsed.message_type, MessageType::PingResponse);
        assert_eq!(parsed.stream_id, 0);
        assert_eq!(&parsed.payload[..], b"beat");
    }

    #[test]
    fn header_i32_widens_signed_values_and_rejects_the_rest() {
        assert_eq!(header_i32(&HeaderValue::Int16(-1)), Some(-1));
        assert_eq!(header_i32(&HeaderValue::Int64(-1)), Some(-1));
        assert_eq!(
            header_i32(&HeaderValue::Int64(i64::from(i32::MAX) + 1)),
            None
        );
        assert_eq!(header_i32(&HeaderValue::Bool(true)), None);
    }

    #[test]
    fn terminate_sets_flag() {
        let msg = RpcMessage::terminate(3);
        let parsed = RpcMessage::parse(&msg).unwrap();
        assert!(parsed.terminates_stream());
        assert_eq!(parsed.stream_id, 3);
    }
}
