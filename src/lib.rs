//! Data structures representing the mux protocol.
//!
//! Package mux implements a generic RPC multiplexer with a rich protocol.
//! Mux is itself encoding independent, so it is meant to use as the
//! transport for other RPC systems (eg. thrift). In OSI terminology, it
//! is a pure session layer.

extern crate byteorder;

mod dtab;
pub mod codec;
pub mod session;
pub mod types;

pub use dtab::*;
use std::time::Duration;

/// Headers for a `Treq`.
pub type Headers = Vec<(u8, Vec<u8>)>;

/// Contexts of dispatch and init messages.
pub type Contexts = Vec<(Vec<u8>, Vec<u8>)>;

/// Maximum value of a mux Tag
pub const MAX_TAG: u32 = (1 << 23) - 1;

/// Id number and end flag for message frames.
///
/// Every message has an associated `Tag` following the frame length and frame
/// type on the wire. The frame id represents the stream identifier and is limited
/// to 23 bits of precision while bit 24 signals if the message stream is ending.
/// This only applies to the `Tdispatch` and `Rdispatch` frames.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Tag {
    /// Signal that this frame is the end of the stream of fragments.
    ///
    /// Currently, only Tdispatch and Rdispatch messages may be split into an
    /// ordered sequence of fragments. TdispatchError message ends a Tdispatch
    /// sequence and an Rerr ends an Rdispatch sequence.
    pub end: bool,
    /// Identification number associated with this stream.
    pub id: u32,
}

/// Representation of an entire mux packet.
#[derive(Debug, PartialEq, Eq)]
pub struct Message {
    /// Identification and termination information about the associated stream.
    pub tag: Tag,
    /// Payload of the message. The length is determined from the payload.
    pub frame: MessageFrame,
}

/// Type wrapper for the mux packet representations.
#[derive(Debug, PartialEq, Eq)]
pub enum MessageFrame {
    Treq(Treq),
    Rreq(Rmsg),
    Tdispatch(Tdispatch),
    Rdispatch(Rdispatch),
    Tinit(Init),
    Rinit(Init),
    Tdrain,
    Rdrain,
    Tping,
    Rping,
    Tdiscarded(Tdiscarded),
    Tlease(Tlease),
    Rerr(Rerr),
}

// Structs that model the message frame types of the mux protocol

/// Representation of the mux `Treq` types.
#[derive(PartialEq, Eq, Debug)]
pub struct Treq {
    /// Request headers.
    pub headers: Headers,
    /// Body of the request.
    pub body: Vec<u8>,
}

/// Representation of a mux `Rreq` and `Rdispatch` message body.
#[derive(PartialEq, Eq, Debug)]
pub enum Rmsg {
    /// Successful response containing a body.
    Ok(Vec<u8>),
    /// Response failed. The `String` describes the error.
    Error(String),
    /// Negative acknowledgment. The `String` describes the reason.
    Nack(String),
}

/// Representation of a mux `Tdispatch` frame.
#[derive(PartialEq, Eq, Debug)]
pub struct Tdispatch {
    /// Context information associated with this request.
    pub contexts: Contexts,
    /// Destination of this request.
    pub dest: String,
    /// Table of delegation rules for 'rewriting' the destination.
    pub dtab: Dtab,
    /// Message payload.
    pub body: Vec<u8>,
}

/// Representation of a mux `Rdispatch` frame.
#[derive(PartialEq, Eq, Debug)]
pub struct Rdispatch {
    /// Context information associated with this request.
    pub contexts: Contexts,
    /// Response of the dispatch request.
    pub msg: Rmsg,
}

/// Representation of a mux `Tinit` and `Rinit` frame.
///
/// `Tinit` and `Rinit` frames are used for negotiation of the mux protocol
/// version and behavior. A `Tinit` is typically sent by the client at the
/// beginning of the session. Until a `Rinit` is received the client cannot
/// issue any more T messages. Once the `Rinit` is received, the session state
/// is considered reset. The version return in `Rinit` is the accepted protocol
/// version and may be lower than that of the issued `Tinit`.
#[derive(PartialEq, Eq, Debug)]
pub struct Init {
    /// Mux protocol version.
    pub version: u16,
    /// Additional negotiation related information.
    pub headers: Contexts,
}

/// Representation of a mux `Tdiscarded` frame.
///
/// A `Tdiscarded` frame is a marker message alerting the server that the
/// client has discarded the `Tdispatch` issued with the associated id. This
/// does not free the server from the obligation of replying to the origional
/// request.
#[derive(PartialEq, Eq, Debug)]
pub struct Tdiscarded {
    /// Stream id of the discarded `Tdispatch` request.
    pub id: u32,
    /// Reason for discarding the request.
    pub msg: String,
}

/// Representation of a mux `Tlease` frame.
///
/// A `Tlease` is a marker message that is issued to alert the client that
/// it has been allocated resources for a specific duration. In the abscence
/// of a `Tlease`, the client assumes it holds an indefinate lease.
/// Adhering to the lease is optional but the server may reject requests or
/// operate at a degraded capacity under and expired lease.
#[derive(PartialEq, Eq, Debug)]
pub struct Tlease {
    /// `Duration` of the lease allocated to the client.
    pub duration: Duration,
}

/// Representation of a mux `Rerr` frame.
///
/// An `Rerr` is sent from the server in the even that the server failed to
/// interpret or act on a request T message.
#[derive(PartialEq, Eq, Debug)]
pub struct Rerr {
    /// Description of the error.
    pub msg: String,
}

impl Tag {
    #[inline]
    /// Construct a new `Tag`.
    pub fn new(end: bool, id: u32) -> Tag {
        assert!(id <= MAX_TAG);
        Tag { end: end, id: id, }
    }
}

impl MessageFrame {
    /// Get the `i8` value coresponding the a `MessageFrame`.
    pub fn frame_id(&self) -> i8 {
        match *self {
            MessageFrame::Treq(_) => types::TREQ,
            MessageFrame::Rreq(_) => types::RREQ,
            MessageFrame::Tdispatch(_) => types::TDISPATCH,
            MessageFrame::Rdispatch(_) => types::RDISPATCH,
            MessageFrame::Tinit(_) => types::TINIT,
            MessageFrame::Rinit(_) => types::RINIT,
            MessageFrame::Tdrain => types::TDRAIN,
            MessageFrame::Rdrain => types::RDRAIN,
            MessageFrame::Tping => types::TPING,
            MessageFrame::Rping => types::RPING,
            MessageFrame::Tdiscarded(_) => types::TDISCARDED,
            MessageFrame::Tlease(_) => types::TLEASE,
            MessageFrame::Rerr(_) => types::RERR,
        }
    }
}

impl Tdispatch {
    /// Construct a new `Tdispatch` frame with the provided destination and body.
    pub fn new(dest: String, body: Vec<u8>) -> Tdispatch {
        Tdispatch {
            contexts: Vec::new(),
            dest: dest,
            dtab: Dtab::new(),
            body: body,
        }
    }
}
