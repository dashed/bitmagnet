//! Pure BitTorrent DHT wire contracts.

mod compact;
mod inbound;
mod krpc;
mod receive;
mod routing_tree;
mod scrape;
mod transaction;

pub use compact::{CompactAddr, CompactCodecError, CompactNode, Id20};
pub use inbound::{
    InboundError, InboundLimitKind, InboundShapeKind, InboundSyntaxKind,
    MAX_INBOUND_DATAGRAM_BYTES, MAX_INBOUND_NESTING_DEPTH, MAX_INBOUND_VALUES,
};
pub use krpc::{ByteString, KrpcError, KrpcMessage, MessageArgs, MessageReturn, WireError};
pub use receive::{
    DatagramReceiver, ReceiveDispatchError, ReceiveDispatchOutcome, ReceiveDispatcher,
    ReceivedDatagram,
};
pub use routing_tree::{RoutingPutResult, RoutingTree, ROUTING_ID_BITS};
pub use scrape::{ScrapeBloomError, ScrapeBloomFilter, SCRAPE_BLOOM_BYTES};
pub use transaction::{
    CryptoTransactionIdIssuer, DeliveryOutcome, PendingTransaction, RegisterError,
    RegisterSendError, RegisteredQuery, TransactionId, TransactionIdError, TransactionIdIssuer,
    TransactionIdSourceError, TransactionRegistry, TransactionWaitOutcome,
};
