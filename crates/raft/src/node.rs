use std::collections::{BTreeMap, VecDeque};

use crate::rng::Rng;
use crate::storage::Storage;
use crate::types::*;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// Followers campaign after a random timeout in `[election_ticks, 2 * election_ticks)`.
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    pub max_append_entries: usize,
    /// Probe with a non-binding PreVote round first, so a rejoining node cannot bump terms.
    pub pre_vote: bool,
    /// Leader steps down without a quorum for an election timeout; followers in contact
    /// with a leader ignore vote requests (leader stickiness).
    pub check_quorum: bool,
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_entries: 512,
            pre_vote: true,
            check_quorum: true,
            seed: id,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Progress {
    next: u64,
    matched: u64,
    /// Highest ReadIndex ctx this follower acknowledged in the current term.
    ctx: u64,
    active: bool,
}

#[derive(Debug)]
struct PendingRead {
    token: u64,
    index: u64,
    ctx: u64,
}

#[derive(Clone, Debug)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub commit: u64,
    pub applied: u64,
    pub last_index: u64,
    pub snapshot_index: u64,
    pub members: Vec<Member>,
}

/// Sans-IO Raft: callers feed ticks, messages and proposals, then drain [`Ready`].
pub struct RaftNode<S: Storage> {
    cfg: Config,
    storage: S,
    rng: Rng,

    term: u64,
    vote: Option<NodeId>,
    role: Role,
    leader: Option<NodeId>,

    /// Entries after `snapshot.meta.index`.
    log: Vec<Entry>,
    snapshot: Snapshot,
    commit: u64,
    applied: u64,
    members: Vec<Member>,
    /// Index of the latest config entry in the log (0 if the config came from the snapshot).
    config_index: u64,

    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,

    progress: BTreeMap<NodeId, Progress>,
    votes: BTreeMap<NodeId, bool>,
    noop_index: u64,
    read_ctx: u64,
    reads: VecDeque<PendingRead>,

    out: Vec<Message>,
    ready_reads: Vec<(u64, u64)>,
    aborted_reads: Vec<u64>,
    snapshot_to_apply: Option<Snapshot>,
    need_broadcast: bool,
    need_heartbeat: bool,
}

impl<S: Storage> RaftNode<S> {
    /// Restores from `storage`. Use [`crate::bootstrap`] first for a new cluster; a node with
    /// no members waits to be added by a leader.
    pub fn new(cfg: Config, mut storage: S) -> Self {
        let (hs, snapshot, log) = storage.initial_state();
        let mut n = Self {
            rng: Rng::new(cfg.seed),
            cfg,
            storage,
            term: hs.term,
            vote: hs.vote,
            role: Role::Follower,
            leader: None,
            log,
            commit: snapshot.meta.index,
            applied: snapshot.meta.index,
            snapshot_to_apply: (snapshot.meta.index > 0).then(|| snapshot.clone()),
            snapshot,
            members: Vec::new(),
            config_index: 0,
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            progress: BTreeMap::new(),
            votes: BTreeMap::new(),
            noop_index: 0,
            read_ctx: 0,
            reads: VecDeque::new(),
            out: Vec::new(),
            ready_reads: Vec::new(),
            aborted_reads: Vec::new(),
            need_broadcast: false,
            need_heartbeat: false,
        };
        n.refresh_members();
        n.reset_election_timer();
        n
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.meta.index
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.cfg.id,
            role: self.role,
            term: self.term,
            leader: self.leader,
            commit: self.commit,
            applied: self.applied,
            last_index: self.last_index(),
            snapshot_index: self.snapshot.meta.index,
            members: self.members.clone(),
        }
    }

    // ---- inputs -------------------------------------------------------------------------

    pub fn tick(&mut self) {
        self.election_elapsed += 1;
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.election_elapsed >= self.cfg.election_ticks {
                self.election_elapsed = 0;
                if self.cfg.check_quorum && !self.quorum_active() {
                    self.become_follower(self.term, None);
                    return;
                }
                for p in self.progress.values_mut() {
                    p.active = false;
                }
            }
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.need_heartbeat = true;
            }
        } else if self.election_elapsed >= self.election_timeout && self.is_voter(self.cfg.id) {
            self.campaign(self.cfg.pre_vote);
        }
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        Ok(self.append_entry(Payload::Command(data)))
    }

    /// Single-server membership change (Raft thesis §4.1). At most one may be uncommitted,
    /// and only after the leader committed an entry in its own term.
    pub fn propose_conf_change(&mut self, change: ConfChange) -> Result<u64, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        if self.term_at(self.commit) != Some(self.term) || self.config_index > self.commit {
            return Err(ProposeError::ConfChangeInProgress);
        }
        let mut members = self.members.clone();
        match change {
            ConfChange::Add(m) => {
                if members.iter().any(|x| x.id == m.id) {
                    return Err(ProposeError::Invalid("already a member"));
                }
                members.push(m);
            }
            ConfChange::Remove(id) => {
                if !members.iter().any(|x| x.id == id) {
                    return Err(ProposeError::Invalid("not a member"));
                }
                if members.len() == 1 {
                    return Err(ProposeError::Invalid("cannot remove the last member"));
                }
                members.retain(|x| x.id != id);
            }
        }
        members.sort_by_key(|m| m.id);
        Ok(self.append_entry(Payload::Config(members)))
    }

    /// Linearizable read (ReadIndex, thesis §6.4). `token` is returned in
    /// [`Ready::reads`] once a quorum confirmed this node is still leader.
    pub fn read_index(&mut self, token: u64) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        // All entries committed before this term are <= noop_index, so waiting for it to
        // apply is enough even before the leader learns its own commit index.
        let index = self.commit.max(self.noop_index);
        self.read_ctx += 1;
        self.reads.push_back(PendingRead {
            token,
            index,
            ctx: self.read_ctx,
        });
        self.need_heartbeat = true;
        self.release_reads();
        Ok(())
    }

    /// Replaces the log prefix up to `index` (which must be applied) with `data`.
    pub fn compact(&mut self, index: u64, data: Vec<u8>) {
        if index > self.applied || index <= self.snapshot.meta.index {
            return;
        }
        let meta = SnapshotMeta {
            index,
            term: self.term_at(index).expect("applied entry is in the log"),
            members: self.config_at(index),
        };
        let snap = Snapshot { meta, data };
        self.storage.save_snapshot(&snap);
        self.log
            .drain(..(index - self.snapshot.meta.index) as usize);
        self.snapshot = snap;
    }

    pub fn step(&mut self, m: Message) {
        if m.term > self.term {
            let is_vote = matches!(m.msg, Msg::Vote { .. });
            if is_vote && self.in_lease() {
                return;
            }
            match m.msg {
                Msg::Vote { pre: true, .. }
                | Msg::VoteResp {
                    pre: true,
                    granted: true,
                } => {}
                Msg::Append { .. } | Msg::InstallSnapshot { .. } => {
                    self.become_follower(m.term, Some(m.from))
                }
                _ => self.become_follower(m.term, None),
            }
        } else if m.term < self.term {
            match m.msg {
                // Tell a stale leader or pre-candidate about the newer term.
                Msg::Append { .. } | Msg::InstallSnapshot { .. } => self.send(
                    m.from,
                    Msg::AppendResp {
                        success: false,
                        index: 0,
                        ctx: 0,
                    },
                ),
                Msg::Vote { pre: true, .. } => self.send(
                    m.from,
                    Msg::VoteResp {
                        granted: false,
                        pre: true,
                    },
                ),
                _ => {}
            }
            return;
        }

        match m.msg {
            Msg::Vote {
                last_index,
                last_term,
                pre,
            } => self.handle_vote(m.from, m.term, last_index, last_term, pre),
            Msg::VoteResp { granted, pre } => self.handle_vote_resp(m.from, m.term, granted, pre),
            Msg::Append {
                prev_index,
                prev_term,
                entries,
                commit,
                ctx,
            } => self.handle_append(m.from, prev_index, prev_term, entries, commit, ctx),
            Msg::AppendResp {
                success,
                index,
                ctx,
            } => self.handle_append_resp(m.from, success, index, ctx),
            Msg::InstallSnapshot { snapshot } => self.handle_snapshot(m.from, snapshot),
        }
    }

    /// Drains outputs. Storage is synced before anything is returned.
    pub fn ready(&mut self) -> Ready {
        if self.role == Role::Leader && (self.need_heartbeat || self.need_broadcast) {
            self.broadcast_append(self.need_heartbeat);
        }
        self.need_heartbeat = false;
        self.need_broadcast = false;
        self.storage.sync();

        let snapshot = self.snapshot_to_apply.take();
        let mut committed = Vec::new();
        if self.commit > self.applied {
            let lo = (self.applied - self.snapshot.meta.index) as usize;
            let hi = (self.commit - self.snapshot.meta.index) as usize;
            committed = self.log[lo..hi].to_vec();
            self.applied = self.commit;
        }
        Ready {
            messages: std::mem::take(&mut self.out),
            snapshot,
            committed,
            reads: std::mem::take(&mut self.ready_reads),
            aborted_reads: std::mem::take(&mut self.aborted_reads),
        }
    }

    // ---- elections ----------------------------------------------------------------------

    fn in_lease(&self) -> bool {
        self.cfg.check_quorum
            && self.leader.is_some()
            && self.election_elapsed < self.cfg.election_ticks
    }

    fn campaign(&mut self, pre: bool) {
        self.reset_election_timer();
        self.leader = None;
        self.votes.clear();
        let term = if pre {
            self.role = Role::PreCandidate;
            self.term + 1
        } else {
            self.role = Role::Candidate;
            self.term += 1;
            self.vote = Some(self.cfg.id);
            self.persist_hard_state();
            self.term
        };
        self.votes.insert(self.cfg.id, true);
        if self.tally().0 >= self.quorum() {
            return self.won_vote(pre);
        }
        let (last_index, last_term) = (self.last_index(), self.last_term());
        for to in self.peer_ids() {
            self.out.push(Message {
                from: self.cfg.id,
                to,
                term,
                msg: Msg::Vote {
                    last_index,
                    last_term,
                    pre,
                },
            });
        }
    }

    fn won_vote(&mut self, pre: bool) {
        if pre {
            self.campaign(false)
        } else {
            self.become_leader()
        }
    }

    fn handle_vote(&mut self, from: NodeId, term: u64, last_index: u64, last_term: u64, pre: bool) {
        let up_to_date = (last_term, last_index) >= (self.last_term(), self.last_index());
        let can_vote = if pre {
            term > self.term
        } else {
            self.vote == Some(from) || (self.vote.is_none() && self.leader.is_none())
        };
        if can_vote && up_to_date {
            if !pre {
                self.vote = Some(from);
                self.persist_hard_state();
                self.reset_election_timer();
            }
            self.out.push(Message {
                from: self.cfg.id,
                to: from,
                term,
                msg: Msg::VoteResp { granted: true, pre },
            });
        } else {
            self.send(
                from,
                Msg::VoteResp {
                    granted: false,
                    pre,
                },
            );
        }
    }

    fn handle_vote_resp(&mut self, from: NodeId, term: u64, granted: bool, pre: bool) {
        let expected = match (self.role, pre) {
            (Role::PreCandidate, true) => term == self.term + 1 || (!granted && term == self.term),
            (Role::Candidate, false) => term == self.term,
            _ => false,
        };
        if !expected {
            return;
        }
        self.votes.insert(from, granted);
        let (yes, no) = self.tally();
        if yes >= self.quorum() {
            self.won_vote(pre);
        } else if no > self.members.len() - self.quorum() {
            self.become_follower(self.term, None);
        }
    }

    fn tally(&self) -> (usize, usize) {
        self.members
            .iter()
            .filter_map(|m| self.votes.get(&m.id))
            .fold((0, 0), |(y, n), &g| if g { (y + 1, n) } else { (y, n + 1) })
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.persist_hard_state();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.progress.clear();
        self.aborted_reads
            .extend(self.reads.drain(..).map(|r| r.token));
        self.reset_election_timer();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.read_ctx = 0;
        self.progress.clear();
        self.sync_progress();
        self.noop_index = self.append_entry(Payload::Noop);
        self.need_heartbeat = true;
    }

    // ---- replication --------------------------------------------------------------------

    fn append_entry(&mut self, payload: Payload) -> u64 {
        let entry = Entry {
            term: self.term,
            index: self.last_index() + 1,
            payload,
        };
        let is_config = matches!(entry.payload, Payload::Config(_));
        self.storage.append(std::slice::from_ref(&entry));
        self.log.push(entry);
        if is_config {
            self.refresh_members();
        }
        self.need_broadcast = true;
        self.maybe_commit();
        self.last_index()
    }

    fn broadcast_append(&mut self, all: bool) {
        let last = self.last_index();
        let peers: Vec<NodeId> = self
            .progress
            .iter()
            .filter(|(_, p)| all || p.next <= last)
            .map(|(id, _)| *id)
            .collect();
        for to in peers {
            self.send_append(to);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let snap_index = self.snapshot.meta.index;
        let last = self.last_index();
        let Some(pr) = self.progress.get_mut(&to) else {
            return;
        };
        if pr.next <= snap_index {
            pr.next = snap_index + 1;
            let snapshot = self.snapshot.clone();
            return self.send(to, Msg::InstallSnapshot { snapshot });
        }
        let prev_index = pr.next - 1;
        let hi = last.min(prev_index + self.cfg.max_append_entries as u64);
        // Optimistic pipelining: a lost message surfaces as a rejection on the next round.
        pr.next = hi + 1;
        let lo = (prev_index - snap_index) as usize;
        let entries = self.log[lo..(hi - snap_index) as usize].to_vec();
        let msg = Msg::Append {
            prev_index,
            prev_term: self.term_at(prev_index).expect("prev is after snapshot"),
            entries,
            commit: self.commit,
            ctx: self.read_ctx,
        };
        self.send(to, msg);
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        mut prev_index: u64,
        mut prev_term: u64,
        mut entries: Vec<Entry>,
        leader_commit: u64,
        ctx: u64,
    ) {
        if self.role != Role::Follower || self.leader != Some(from) {
            self.become_follower(self.term, Some(from));
        }
        self.election_elapsed = 0;

        // Our committed prefix matches the leader's; skip it (it may also be compacted).
        if prev_index < self.commit {
            let skip = ((self.commit - prev_index) as usize).min(entries.len());
            entries.drain(..skip);
            prev_index = self.commit;
            prev_term = self.term_at(self.commit).expect("commit is in log");
        }

        if self.term_at(prev_index) != Some(prev_term) {
            let last = self.last_index();
            let hint = if prev_index > last {
                last
            } else {
                // Skip the whole conflicting term in one round trip.
                let conflict = self.term_at(prev_index);
                let mut i = prev_index - 1;
                while i > self.commit && self.term_at(i) == conflict {
                    i -= 1;
                }
                i
            };
            return self.send(
                from,
                Msg::AppendResp {
                    success: false,
                    index: hint,
                    ctx,
                },
            );
        }

        let last_new = prev_index + entries.len() as u64;
        if let Some(pos) = entries
            .iter()
            .position(|e| self.term_at(e.index) != Some(e.term))
        {
            let first = entries[pos].index;
            // Config is recomputed (a log scan) only if a config entry is removed or added.
            let mut config_changed = first <= self.config_index;
            if first <= self.last_index() {
                self.storage.truncate(first);
                self.log
                    .truncate((first - self.snapshot.meta.index - 1) as usize);
            }
            let new = &entries[pos..];
            config_changed |= new.iter().any(|e| matches!(e.payload, Payload::Config(_)));
            self.storage.append(new);
            self.log.extend_from_slice(new);
            if config_changed {
                self.refresh_members();
            }
        }
        let commit = leader_commit.min(last_new);
        if commit > self.commit {
            self.commit = commit;
        }
        self.send(
            from,
            Msg::AppendResp {
                success: true,
                index: last_new,
                ctx,
            },
        );
    }

    fn handle_append_resp(&mut self, from: NodeId, success: bool, index: u64, ctx: u64) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.last_index();
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        pr.active = true;
        pr.ctx = pr.ctx.max(ctx);
        if success {
            pr.matched = pr.matched.max(index);
            pr.next = pr.next.max(index + 1);
            let behind = pr.next <= last;
            self.maybe_commit();
            if behind {
                self.send_append(from);
            }
        } else if index >= pr.matched {
            pr.next = pr.next.min(index + 1).max(pr.matched + 1);
            self.send_append(from);
        }
        self.release_reads();
    }

    fn handle_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        if self.role != Role::Follower || self.leader != Some(from) {
            self.become_follower(self.term, Some(from));
        }
        self.election_elapsed = 0;
        let index = snapshot.meta.index;
        if index <= self.commit {
            let commit = self.commit;
            return self.send(
                from,
                Msg::AppendResp {
                    success: true,
                    index: commit,
                    ctx: 0,
                },
            );
        }
        if self.term_at(index) == Some(snapshot.meta.term) {
            self.log
                .drain(..(index - self.snapshot.meta.index) as usize);
        } else {
            self.storage.truncate(self.snapshot.meta.index + 1);
            self.log.clear();
        }
        self.storage.save_snapshot(&snapshot);
        self.commit = index;
        self.applied = index;
        self.snapshot_to_apply = Some(snapshot.clone());
        self.snapshot = snapshot;
        self.refresh_members();
        self.send(
            from,
            Msg::AppendResp {
                success: true,
                index,
                ctx: 0,
            },
        );
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader || self.members.is_empty() {
            return;
        }
        let last = self.last_index();
        let mut matched: Vec<u64> = self
            .members
            .iter()
            .map(|m| {
                if m.id == self.cfg.id {
                    last
                } else {
                    self.progress.get(&m.id).map_or(0, |p| p.matched)
                }
            })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.quorum() - 1];
        // Only entries from the current term commit by counting (§5.4.2).
        if n > self.commit && self.term_at(n) == Some(self.term) {
            self.commit = n;
            self.need_broadcast = true;
            if !self.is_voter(self.cfg.id) && self.config_index <= self.commit {
                // Our removal is committed: hand off by stepping down.
                self.become_follower(self.term, None);
            }
        }
    }

    fn release_reads(&mut self) {
        if self.reads.is_empty() {
            return;
        }
        let mut acked: Vec<u64> = self
            .members
            .iter()
            .map(|m| {
                if m.id == self.cfg.id {
                    self.read_ctx
                } else {
                    self.progress.get(&m.id).map_or(0, |p| p.ctx)
                }
            })
            .collect();
        if acked.is_empty() {
            return;
        }
        acked.sort_unstable_by(|a, b| b.cmp(a));
        let confirmed = acked[self.quorum() - 1];
        while self.reads.front().is_some_and(|r| r.ctx <= confirmed) {
            let r = self.reads.pop_front().expect("checked");
            self.ready_reads.push((r.token, r.index));
        }
    }

    fn quorum_active(&self) -> bool {
        let active = self
            .members
            .iter()
            .filter(|m| m.id == self.cfg.id || self.progress.get(&m.id).is_some_and(|p| p.active))
            .count();
        active >= self.quorum()
    }

    // ---- membership ---------------------------------------------------------------------

    fn config_at(&self, index: u64) -> Vec<Member> {
        if self.config_index <= index {
            return self.members.clone();
        }
        self.log
            .iter()
            .rev()
            .filter(|e| e.index <= index)
            .find_map(|e| match &e.payload {
                Payload::Config(m) => Some(m.clone()),
                _ => None,
            })
            .unwrap_or_else(|| self.snapshot.meta.members.clone())
    }

    fn refresh_members(&mut self) {
        let latest = self.log.iter().rev().find_map(|e| match &e.payload {
            Payload::Config(m) => Some((e.index, m.clone())),
            _ => None,
        });
        (self.config_index, self.members) =
            latest.unwrap_or_else(|| (0, self.snapshot.meta.members.clone()));
        if self.role == Role::Leader {
            self.sync_progress();
        }
    }

    fn sync_progress(&mut self) {
        let next = self.last_index() + 1;
        let ids: Vec<NodeId> = self.peer_ids();
        self.progress.retain(|id, _| ids.contains(id));
        for id in ids {
            self.progress.entry(id).or_insert(Progress {
                next,
                active: true,
                ..Default::default()
            });
        }
    }

    // ---- helpers ------------------------------------------------------------------------

    fn peer_ids(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .map(|m| m.id)
            .filter(|&id| id != self.cfg.id)
            .collect()
    }

    fn is_voter(&self, id: NodeId) -> bool {
        self.members.iter().any(|m| m.id == id)
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    fn last_index(&self) -> u64 {
        self.snapshot.meta.index + self.log.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(self.snapshot.meta.term, |e| e.term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        let base = self.snapshot.meta.index;
        if index == base {
            Some(self.snapshot.meta.term)
        } else if index < base || index > self.last_index() {
            None
        } else {
            Some(self.log[(index - base - 1) as usize].term)
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let t = self.cfg.election_ticks;
        self.election_timeout = self.rng.range(t, 2 * t);
    }

    fn persist_hard_state(&mut self) {
        self.storage.set_hard_state(HardState {
            term: self.term,
            vote: self.vote,
        });
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.out.push(Message {
            from: self.cfg.id,
            to,
            term: self.term,
            msg,
        });
    }
}
