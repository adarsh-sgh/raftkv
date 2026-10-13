//! Core types <-> protobuf (wire and disk format).

use crate::pb;
use raft::{Entry, HardState, Member, Message, Msg, Payload, Snapshot, SnapshotMeta};

pub fn member_to_pb(m: &Member) -> pb::Member {
    pb::Member {
        id: m.id,
        addr: m.addr.clone(),
    }
}

pub fn member_from_pb(m: pb::Member) -> Member {
    Member {
        id: m.id,
        addr: m.addr,
    }
}

pub fn entry_to_pb(e: &Entry) -> pb::Entry {
    use pb::entry::Payload as P;
    let payload = match &e.payload {
        Payload::Noop => P::Noop(true),
        Payload::Command(c) => P::Command(c.clone()),
        Payload::Config(m) => P::Config(pb::Config {
            members: m.iter().map(member_to_pb).collect(),
        }),
    };
    pb::Entry {
        term: e.term,
        index: e.index,
        payload: Some(payload),
    }
}

pub fn entry_from_pb(e: pb::Entry) -> Entry {
    use pb::entry::Payload as P;
    let payload = match e.payload {
        None | Some(P::Noop(_)) => Payload::Noop,
        Some(P::Command(c)) => Payload::Command(c),
        Some(P::Config(c)) => Payload::Config(c.members.into_iter().map(member_from_pb).collect()),
    };
    Entry {
        term: e.term,
        index: e.index,
        payload,
    }
}

pub fn hard_state_to_pb(hs: HardState) -> pb::HardState {
    pb::HardState {
        term: hs.term,
        vote: hs.vote,
    }
}

pub fn hard_state_from_pb(hs: pb::HardState) -> HardState {
    HardState {
        term: hs.term,
        vote: hs.vote,
    }
}

pub fn snapshot_to_pb(s: &Snapshot) -> pb::Snapshot {
    pb::Snapshot {
        meta: Some(pb::SnapshotMeta {
            index: s.meta.index,
            term: s.meta.term,
            members: s.meta.members.iter().map(member_to_pb).collect(),
        }),
        data: s.data.clone(),
    }
}

pub fn snapshot_from_pb(s: pb::Snapshot) -> Snapshot {
    let meta = s.meta.unwrap_or_default();
    Snapshot {
        meta: SnapshotMeta {
            index: meta.index,
            term: meta.term,
            members: meta.members.into_iter().map(member_from_pb).collect(),
        },
        data: s.data,
    }
}

pub fn message_to_pb(m: Message, from_addr: &str) -> pb::RaftMessage {
    use pb::raft_message::Msg as M;
    let msg = match m.msg {
        Msg::Vote {
            last_index,
            last_term,
            pre,
        } => M::Vote(pb::Vote {
            last_index,
            last_term,
            pre,
        }),
        Msg::VoteResp { granted, pre } => M::VoteResp(pb::VoteResp { granted, pre }),
        Msg::Append {
            prev_index,
            prev_term,
            entries,
            commit,
            ctx,
        } => M::Append(pb::Append {
            prev_index,
            prev_term,
            entries: entries.iter().map(entry_to_pb).collect(),
            commit,
            ctx,
        }),
        Msg::AppendResp {
            success,
            index,
            ctx,
        } => M::AppendResp(pb::AppendResp {
            success,
            index,
            ctx,
        }),
        Msg::InstallSnapshot { snapshot } => M::InstallSnapshot(snapshot_to_pb(&snapshot)),
    };
    pb::RaftMessage {
        from: m.from,
        to: m.to,
        term: m.term,
        from_addr: from_addr.to_string(),
        msg: Some(msg),
    }
}

pub fn message_from_pb(m: pb::RaftMessage) -> Option<Message> {
    use pb::raft_message::Msg as M;
    let msg = match m.msg? {
        M::Vote(v) => Msg::Vote {
            last_index: v.last_index,
            last_term: v.last_term,
            pre: v.pre,
        },
        M::VoteResp(v) => Msg::VoteResp {
            granted: v.granted,
            pre: v.pre,
        },
        M::Append(a) => Msg::Append {
            prev_index: a.prev_index,
            prev_term: a.prev_term,
            entries: a.entries.into_iter().map(entry_from_pb).collect(),
            commit: a.commit,
            ctx: a.ctx,
        },
        M::AppendResp(a) => Msg::AppendResp {
            success: a.success,
            index: a.index,
            ctx: a.ctx,
        },
        M::InstallSnapshot(s) => Msg::InstallSnapshot {
            snapshot: snapshot_from_pb(s),
        },
    };
    Some(Message {
        from: m.from,
        to: m.to,
        term: m.term,
        msg,
    })
}
