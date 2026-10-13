pub type NodeId = u64;

/// A voting member. `addr` is opaque to the core; the server uses it to dial peers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Member {
    pub id: NodeId,
    pub addr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Payload {
    /// Appended by every new leader so it can commit (and serve reads) in its term.
    Noop,
    Command(Vec<u8>),
    /// Full voter set. Takes effect as soon as it is appended (single-server changes).
    Config(Vec<Member>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub payload: Payload,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<NodeId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
    pub members: Vec<Member>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    Vote {
        last_index: u64,
        last_term: u64,
        pre: bool,
    },
    VoteResp {
        granted: bool,
        pre: bool,
    },
    Append {
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
        /// ReadIndex round the leader is confirming; echoed back in the response.
        ctx: u64,
    },
    /// On success `index` is the follower's match index, on rejection a hint for `next - 1`.
    AppendResp {
        success: bool,
        index: u64,
        ctx: u64,
    },
    InstallSnapshot {
        snapshot: Snapshot,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: u64,
    pub msg: Msg,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfChange {
    Add(Member),
    Remove(NodeId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    /// A config change is uncommitted, or the leader has not committed in its term yet.
    ConfChangeInProgress,
    Invalid(&'static str),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader(l) => write!(f, "not leader (hint {l:?})"),
            Self::ConfChangeInProgress => write!(f, "config change in progress"),
            Self::Invalid(s) => write!(f, "invalid: {s}"),
        }
    }
}

impl std::error::Error for ProposeError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// Output of [`crate::RaftNode::ready`]. Everything in it is already durable in storage.
#[derive(Debug, Default)]
pub struct Ready {
    pub messages: Vec<Message>,
    /// Restore the state machine from this before applying `committed`.
    pub snapshot: Option<Snapshot>,
    pub committed: Vec<Entry>,
    /// Confirmed ReadIndex requests: serve `token` once applied index >= `index`.
    pub reads: Vec<(u64, u64)>,
    /// ReadIndex requests dropped because this node lost leadership.
    pub aborted_reads: Vec<u64>,
}
