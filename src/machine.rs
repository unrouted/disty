use log::{debug, warn};
use prometheus_client::encoding::text::Encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use rand::{thread_rng, Rng};
use std::collections::HashMap;

use crate::{config::Configuration, types::RegistryAction};

const ELECTION_TICK_LOW: u64 = 150;
const ELECTION_TICK_HIGH: u64 = 300;
const HEARTBEAT_TICK: u64 = (ELECTION_TICK_LOW / 20) / 1000;

fn find_first_inconsistency(ours: Vec<LogEntry>, theirs: Vec<LogEntry>) -> u64 {
    for (i, (our_entry, their_entry)) in ours.iter().zip(theirs.iter()).enumerate() {
        if our_entry.term != their_entry.term {
            return i as u64;
        }
    }

    std::cmp::min(ours.len() as u64, theirs.len() as u64)
}

fn get_next_election_tick() -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let mut rng = thread_rng();
    let millis = rng.gen_range(ELECTION_TICK_LOW..ELECTION_TICK_HIGH);
    let duration = std::time::Duration::from_millis(millis);
    now + duration
}

fn get_next_heartbeat_tick() -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let duration = std::time::Duration::from_millis(HEARTBEAT_TICK);
    now + duration
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Message {
    Tick {},
    Vote {
        index: u64,
    },
    VoteReply {
        reject: bool,
    },
    PreVote {
        index: u64,
    },
    PreVoteReply {
        reject: bool,
    },
    AppendEntries {
        leader_commit: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
    },
    AppendEntriesReply {
        reject: bool,
        log_index: Option<u64>,
    },
    AddEntries {
        entries: Vec<LogEntry>,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Envelope {
    pub source: String,
    pub destination: String,
    pub term: u64,
    pub message: Message,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PeerState {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

struct Peer {
    pub identifier: String,
    next_index: u64,
    match_index: u64,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub entry: RegistryAction,
}

#[derive(Default)]
pub struct Log {
    pub entries: Vec<LogEntry>,
    snapshot_index: Option<u64>,
    snapshot_term: Option<u64>,
    truncate_index: Option<u64>,
}

impl<Idx> std::ops::Index<Idx> for Log
where
    Idx: std::slice::SliceIndex<[LogEntry]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.entries[index]
    }
}

impl Log {
    fn load(&mut self, log: Vec<LogEntry>) {
        self.entries.clear();
        self.entries.extend(log.iter().cloned());
    }

    fn last_index(&self) -> u64 {
        let snapshot_index = self.snapshot_index.unwrap_or(0);

        snapshot_index + self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        match self.entries.last() {
            Some(entry) => entry.term,
            None => self.snapshot_term.unwrap_or(0),
        }
    }

    fn truncate(&mut self, index: u64) {
        self.truncate_index = Some(index);

        while self.last_index() >= index {
            if self.entries.pop().is_none() {
                break;
            }
        }
    }

    fn get(&self, index: u64) -> LogEntry {
        let snapshot_index = match self.snapshot_index {
            Some(index) => index,
            _ => 0,
        };

        let adjusted_index = index - snapshot_index - 1;

        match self.entries.iter().nth(adjusted_index.try_into().unwrap()) {
            Some(entry) => entry.clone(),
            _ => {
                panic!("Log probably corrupt");
            }
        }
    }

    fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    fn step(&mut self) {
        self.truncate_index = None;
    }

    /*
        def __getitem__(self, key):
            if isinstance(key, slice):
                new_slice = slice(
                    key.start - 1 if key.start else None,
                    key.stop - 1 if key.stop else None,
                    key.step,
                )
                return self._log[new_slice]

            return self._log[key - 1]
    */
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
struct MachineMetricLabels {
    identifier: String,
}
pub struct Machine {
    pub identifier: String,
    pub state: PeerState,
    pub term: u64,
    pub outbox: Vec<Envelope>,
    pub leader: Option<String>,
    peers: HashMap<String, Peer>,
    voted_for: Option<String>,
    pub tick: tokio::time::Instant,
    vote_count: usize,
    pub commit_index: u64,
    obedient: bool,
    pub log: Log,
    last_applied_index: Family<MachineMetricLabels, Gauge>,
    last_committed: Family<MachineMetricLabels, Gauge>,
    last_saved: Family<MachineMetricLabels, Gauge>,
    last_term_saved: Family<MachineMetricLabels, Gauge>,
    current_term: Family<MachineMetricLabels, Gauge>,
    current_state: Family<MachineMetricLabels, Gauge>,
}

impl Machine {
    pub fn new(config: Configuration, registry: &mut Registry) -> Self {
        let last_applied_index = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_applied_index",
            "Last index that was applied",
            Box::new(last_applied_index.clone()),
        );

        let last_committed = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_committed",
            "Last index that was committed",
            Box::new(last_committed.clone()),
        );

        let last_saved = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_saved",
            "Last index that was stored in the commit log",
            Box::new(last_saved.clone()),
        );

        let last_term_saved = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_saved_term",
            "Last term that was stored in the commit log",
            Box::new(last_term_saved.clone()),
        );

        let current_term = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_current_term",
            "The current term for a node",
            Box::new(current_term.clone()),
        );

        let current_state = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_current_state",
            "The current state for a node",
            Box::new(current_state.clone()),
        );

        let mut peers = HashMap::new();
        for peer in config.peers {
            if peer.name != config.identifier {
                peers.insert(
                    peer.name.clone(),
                    Peer {
                        identifier: peer.name.clone(),
                        next_index: 0,
                        match_index: 0,
                    },
                );
            }
        }

        Machine {
            log: Log::default(),
            identifier: config.identifier,
            state: PeerState::Follower {},
            leader: None,
            term: 1,
            tick: tokio::time::Instant::now(),
            vote_count: 0,
            voted_for: None,
            obedient: true,
            outbox: vec![],
            commit_index: 0,
            peers,
            last_applied_index,
            last_committed,
            last_saved,
            last_term_saved,
            current_term,
            current_state,
        }
    }

    pub fn cluster_size(&self) -> usize {
        self.peers.len()
    }

    pub fn quorum(&self) -> usize {
        ((self.cluster_size() + 1) / 2) + 1
    }

    fn reset_election_tick(&mut self) {
        self.tick = get_next_election_tick();
    }

    fn reset_heartbeat_tick(&mut self) {
        self.tick = get_next_heartbeat_tick();
    }

    fn reset(&mut self, term: u64) {
        if term != self.term {
            self.term = term
            // self.voted_for = None
        }

        self.leader = None
    }

    fn become_follower(&mut self, term: u64, leader: Option<String>) {
        debug!("Became follower of {leader:?}");

        self.state = PeerState::Follower {};
        self.reset(term);
        self.leader = leader;
        self.reset_election_tick();
    }

    fn become_pre_candidate(&mut self) {
        debug!("Became pre-candidate {}", self.identifier);
        self.state = PeerState::PreCandidate {};
        self.obedient = false;

        if self.quorum() == 1 {
            self.become_candidate();
            return;
        }

        self.vote_count = 1;
        self.reset_election_tick();

        let messages: Vec<Envelope> = self
            .peers
            .values()
            .map(|p| {
                self.envelope(
                    self.term + 1,
                    Message::PreVote {
                        index: self.log.last_index(),
                    },
                    p,
                )
            })
            .collect();
        self.outbox.extend(messages);
    }

    fn become_candidate(&mut self) {
        debug!("Became candidate {}", self.identifier);
        self.state = PeerState::Candidate {};
        self.reset(self.term + 1);
        self.vote_count = 1;
        self.voted_for = Some(self.identifier.clone());

        if self.quorum() == 1 {
            self.become_leader();
            return;
        }

        self.reset_election_tick();

        let messages: Vec<Envelope> = self
            .peers
            .values()
            .map(|p| {
                self.envelope(
                    self.term,
                    Message::Vote {
                        index: self.log.last_index(),
                    },
                    p,
                )
            })
            .collect();
        self.outbox.extend(messages);
    }

    fn become_leader(&mut self) {
        debug!("Became leader {}", self.identifier);
        self.state = PeerState::Leader {};
        self.reset(self.term);
        self.reset_election_tick();

        for peer in self.peers.values_mut() {
            peer.next_index = self.log.last_index() + 1;
            peer.match_index = 0;
        }

        self.append(RegistryAction::Empty);
        self.broadcast_entries();
    }

    fn append(&mut self, entry: RegistryAction) -> bool {
        match self.state {
            PeerState::Leader {} => {
                self.log.append(LogEntry {
                    term: self.term,
                    entry,
                });
                self.maybe_commit();
                true
            }
            _ => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        match self.state {
            PeerState::Leader {} => true,
            _ => false,
        }
    }

    fn leader_active(&self) -> bool {
        match self.state {
            PeerState::Leader {} => true,
            _ => self.obedient,
        }
    }

    fn can_vote(&self, envelope: &Envelope) -> bool {
        let index = match envelope.message {
            Message::PreVote { index } => index,
            _ => return false,
        };

        if self.log.last_term() > envelope.term {
            return false;
        }

        if self.log.last_index() > index {
            return false;
        }

        if let Message::PreVote { index: _ } = envelope.message {
            if envelope.term > self.term {
                return true;
            }
        }

        // We have already voted for this node
        match &self.voted_for {
            Some(voted_for) if voted_for == &envelope.source => {
                return true;
            }
            _ => {}
        }

        // We have not voted, but we think we are leader
        if self.voted_for.is_none() && !matches!(self.state, PeerState::Leader {}) {
            return true;
        }

        // FIXME: Is last_term appropriate here???
        if let Message::PreVote { index: _ } = envelope.message {
            if envelope.term > self.term {
                return true;
            }
        }

        false
    }
    fn maybe_commit(&mut self) -> bool {
        let mut commit_index = 0;
        let mut i = std::cmp::max(self.commit_index, 1);

        while i <= self.log.last_index() {
            if self.log.get(i).term != self.term {
                i += 1;
                continue;
            }

            // Start counting at 1 because we count as a vote
            let mut match_count = 1;
            for peer in self.peers.values() {
                if peer.match_index >= i {
                    match_count += 1
                }
            }

            if match_count >= self.quorum() {
                commit_index = i;
            }

            i += 1;

            if commit_index <= self.commit_index {
                return false;
            }

            self.commit_index = std::cmp::min(self.log.last_index(), commit_index)
        }

        true
    }

    pub fn step(&mut self, envelope: &Envelope) -> Result<(), String> {
        self.outbox.clear();
        self.log.step();

        /*
        def step_term(self, message):
            if message.term == 0:
                # local message
                return

            if message.term > self.term:
                # If we have a leader we should ignore PreVote and Vote
                if message.type in (Message.PreVote, Message.Vote):
                    if self.leader_active:
                        logger.debug("PreVote: sticky leader")
                        return

                # Never change term in response to prevote
                if message.type == Message.PreVote:
                    logger.debug("PreVote: Not bumping term")
                    return

                if message.type == Message.PreVoteReply and not message.reject:
                    # We send pre-votes with a future term, when we become a
                    # candidate we will actually apply it
                    logger.debug("PreVote: not rejected; not bumping")
                    return

                self._become_follower(message.term, message.source)

            elif message.term < self.term:
                if message.type == Message.PreVote:
                    self.reply(message, term=self.term, reject=True)

                logger.debug("Message from old term")
                return
            */

        match envelope.message.clone() {
            Message::AddEntries { entries } => match self.state {
                PeerState::Leader {} => {
                    for entry in entries {
                        self.log.append(entry.clone());
                    }
                    self.maybe_commit();
                    self.broadcast_entries();
                }
                _ => {
                    return Err("Rejected: Not leader".to_string());
                }
            },
            Message::Vote { index: _ } => {
                if !self.can_vote(envelope) {
                    debug!("Vote for {} rejected", envelope.source);
                    self.reply(envelope, self.term, Message::VoteReply { reject: true });
                    return Ok(());
                }

                debug!("Will vote for {}", envelope.source);
                self.reply(envelope, self.term, Message::VoteReply { reject: false });

                self.reset_election_tick();
                self.voted_for = Some(envelope.source.clone());
            }

            Message::VoteReply { reject } => {
                if self.state == PeerState::Candidate && !reject {
                    self.vote_count += 1;

                    if self.vote_count >= self.quorum() {
                        self.become_leader();
                    }
                }
            }

            Message::PreVote { index: _ } => {
                if !self.can_vote(envelope) {
                    debug!("Vote for {} rejected", envelope.source);
                    self.reply(envelope, self.term, Message::PreVoteReply { reject: true });
                    return Ok(());
                }

                debug!("Will prevote for {}", envelope.source);
                self.reply(envelope, self.term, Message::PreVoteReply { reject: false });
            }

            Message::PreVoteReply { reject } => {
                if self.state == PeerState::PreCandidate && !reject {
                    self.vote_count += 1;

                    if self.vote_count >= self.quorum() {
                        self.become_candidate();
                    }
                }
            }

            Message::AppendEntries {
                leader_commit,
                prev_index,
                prev_term,
                entries,
            } => {
                if prev_index > self.log.last_index() {
                    debug!("Leader assumed we had log entry {prev_index} but we do not");
                    self.reply(
                        envelope,
                        self.term,
                        Message::AppendEntriesReply {
                            reject: true,
                            log_index: None,
                        },
                    );
                    return Ok(());
                }

                if prev_index > 0 && self.log.get(prev_index).term != prev_term {
                    warn!("Log not valid - mismatched terms");
                    self.reply(
                        envelope,
                        self.term,
                        Message::AppendEntriesReply {
                            reject: true,
                            log_index: None,
                        },
                    );
                    return Ok(());
                }

                match self.state {
                    PeerState::Follower {} => {
                        self.reset_election_tick();
                    }
                    _ => {
                        self.become_follower(envelope.term, Some(envelope.source.clone()));
                    }
                }

                self.obedient = true;
                self.leader = Some(envelope.source.clone());

                let offset = find_first_inconsistency(
                    self.log[prev_index as usize + 1..].to_vec(),
                    entries.clone(),
                );
                let prev_index = prev_index + offset;
                let entries = entries[offset as usize..].to_vec();

                if self.log.last_index() > prev_index {
                    warn!("Need to truncate log to recover quorum");
                    self.log.truncate(prev_index);
                }

                for entry in entries {
                    self.log.append(entry.clone());
                }

                if leader_commit > self.commit_index {
                    self.commit_index = std::cmp::min(leader_commit, self.log.last_index());
                }

                self.reply(
                    envelope,
                    self.term,
                    Message::AppendEntriesReply {
                        reject: false,
                        log_index: Some(self.log.last_index()),
                    },
                );
            }
            Message::AppendEntriesReply { reject, log_index } => {
                if matches!(self.state, PeerState::Leader) {
                    let mut peer = self.peers.get_mut(&envelope.source).unwrap();

                    if reject {
                        if peer.next_index > 1 {
                            peer.next_index -= 1;
                        }
                        return Ok(());
                    }

                    peer.match_index = std::cmp::min(log_index.unwrap(), self.log.last_index());
                    peer.next_index = peer.match_index + 1;
                    self.maybe_commit();
                }
            }
            Message::Tick {} => {
                match self.state {
                    PeerState::Leader {} => {
                        self.broadcast_entries();
                    }
                    PeerState::Follower {} => {
                        // Heartbeat timeout - time to start thinking about elections
                        self.become_pre_candidate()
                    }
                    PeerState::PreCandidate {} => {
                        // Pre-election timed out before receiving all votes
                        self.become_follower(self.term, None);
                    }
                    PeerState::Candidate {} => {
                        // Election timed out before receiving all votes
                        self.become_follower(self.term, None);
                    }
                }
            }
        }

        Ok(())
    }

    fn envelope(&self, term: u64, message: Message, peer: &Peer) -> Envelope {
        Envelope {
            source: self.identifier.clone(),
            destination: peer.identifier.clone(),
            term,
            message,
        }
    }

    fn send(&mut self, term: u64, message: Message, peer: &Peer) {
        self.outbox.push(self.envelope(term, message, peer))
    }

    fn broadcast_entries(&mut self) {
        let mut messages: Vec<Envelope> = vec![];

        for peer in self.peers.values() {
            let prev_index =
                std::cmp::max(std::cmp::min(peer.next_index - 1, self.log.last_index()), 1);
            let prev_term = self.log.get(prev_index).term;

            messages.push(Envelope {
                source: self.identifier.clone(),
                destination: peer.identifier.clone(),
                term: self.term,
                message: Message::AppendEntries {
                    prev_index,
                    prev_term,
                    entries: self.log[prev_index as usize..].to_vec(),
                    leader_commit: self.commit_index,
                },
            });
        }

        self.outbox.extend(messages);
        self.reset_heartbeat_tick();
    }

    fn reply(&mut self, envelope: &Envelope, term: u64, message: Message) {
        self.outbox.push(Envelope {
            source: self.identifier.clone(),
            destination: envelope.source.clone(),
            term,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{PeerConfig, RaftConfig, RegistryConfig};

    use super::*;

    fn single_node_configuration() -> Configuration {
        Configuration {
            identifier: "node1".to_string(),
            ..Default::default()
        }
    }

    fn single_node_machine() -> Machine {
        let mut registry = <prometheus_client::registry::Registry>::default();
        Machine::new(single_node_configuration(), &mut registry)
    }

    fn cluster_node_configuration() -> Configuration {
        Configuration {
            identifier: "node1".to_string(),
            peers: vec![
                PeerConfig {
                    name: "node1".to_string(),
                    raft: RaftConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                    registry: RegistryConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                },
                PeerConfig {
                    name: "node2".to_string(),
                    raft: RaftConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                    registry: RegistryConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                },
                PeerConfig {
                    name: "node3".to_string(),
                    raft: RaftConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                    registry: RegistryConfig {
                        address: "127.0.0.1".to_string(),
                        port: 80,
                    },
                },
            ],
            ..Default::default()
        }
    }

    fn cluster_node_machine() -> Machine {
        let mut registry = <prometheus_client::registry::Registry>::default();

        Machine::new(cluster_node_configuration(), &mut registry)
    }

    #[test]
    fn single_node_become_leader() {
        let mut m = single_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::Leader);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn cluster_node_become_pre_candidate() {
        let mut m = cluster_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        m.outbox.sort();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);
        assert_eq!(
            m.outbox,
            vec![
                Envelope {
                    source: "node1".to_string(),
                    destination: "node2".to_string(),
                    term: 2,
                    message: Message::PreVote { index: 0 }
                },
                Envelope {
                    source: "node1".to_string(),
                    destination: "node3".to_string(),
                    term: 2,
                    message: Message::PreVote { index: 0 }
                }
            ]
        );
    }

    #[test]
    fn cluster_node_pre_candidate_timeout() {
        let mut m = cluster_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // Next tick occurs after voting period times out

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::Follower);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn cluster_node_become_candidate() {
        let mut m = cluster_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(&Envelope {
            source: "node2".to_string(),
            destination: "node1".to_string(),
            message: Message::PreVoteReply { reject: false },
            term: 0,
        })
        .unwrap();

        m.outbox.sort();

        assert_eq!(m.state, PeerState::Candidate);
        assert_eq!(m.outbox.len(), 2);
        assert_eq!(
            m.outbox,
            vec![
                Envelope {
                    source: "node1".to_string(),
                    destination: "node2".to_string(),
                    term: 2,
                    message: Message::Vote { index: 0 }
                },
                Envelope {
                    source: "node1".to_string(),
                    destination: "node3".to_string(),
                    term: 2,
                    message: Message::Vote { index: 0 }
                }
            ]
        );
    }

    #[test]
    fn cluster_node_candidate_timeout() {
        let mut m = cluster_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(&Envelope {
            source: "node2".to_string(),
            destination: "node1".to_string(),
            message: Message::PreVoteReply { reject: false },
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);
        assert_eq!(m.outbox.len(), 2);

        // But a tick before enough votes means we stay a follower

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::Follower);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn cluster_node_become_leader() {
        let mut m = cluster_node_machine();

        m.step(&Envelope {
            source: "node1".to_string(),
            destination: "node1".to_string(),
            message: Message::Tick {},
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(&Envelope {
            source: "node2".to_string(),
            destination: "node1".to_string(),
            message: Message::PreVoteReply { reject: false },
            term: 0,
        })
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);
        assert_eq!(m.outbox.len(), 2);

        // A single vote lets us become a leader

        m.step(&Envelope {
            source: "node2".to_string(),
            destination: "node1".to_string(),
            message: Message::VoteReply { reject: false },
            term: 0,
        })
        .unwrap();

        m.outbox.sort();

        assert_eq!(m.state, PeerState::Leader);
        assert_eq!(m.outbox.len(), 2);
        assert_eq!(
            m.outbox,
            vec![
                Envelope {
                    source: "node1".to_string(),
                    destination: "node2".to_string(),
                    term: 2,
                    message: Message::AppendEntries {
                        leader_commit: 0,
                        prev_index: 1,
                        prev_term: 2,
                        entries: vec![]
                    }
                },
                Envelope {
                    source: "node1".to_string(),
                    destination: "node3".to_string(),
                    term: 2,
                    message: Message::AppendEntries {
                        leader_commit: 0,
                        prev_index: 1,
                        prev_term: 2,
                        entries: vec![]
                    }
                }
            ]
        );
    }
}

/*
def test_leader_handle_append_entries_reply_success(event_loop):
    m = Machine("node1")
    m.add_peer("node2")
    m.add_peer("node3")

    m.log.append((1, {}))
    m.log.append((1, {}))
    m.log.append((1, {}))

    assert m.log.last_index == 3
    assert m.log.last_term == 1

    m.tick = 0
    m.step(Msg("node1", "node1", Message.Tick, 0))

    m.step(Msg("node2", "node1", Message.PreVoteReply, 1, reject=False))
    m.step(Msg("node3", "node1", Message.PreVoteReply, 1, reject=False))

    m.step(Msg("node2", "node1", Message.VoteReply, 1, reject=False))
    outbox = list(m.outbox)
    m.step(Msg("node3", "node1", Message.VoteReply, 1, reject=False))
    outbox.extend(m.outbox)

    m.step(outbox[0].reply(1, reject=False, log_index=3))
    m.step(outbox[1].reply(1, reject=False, log_index=3))

    assert m.peers["node2"].next_index == 4
    assert m.peers["node2"].match_index == 3

    # Make sure we can't commit what we don't have
    m.step(outbox[0].reply(1, reject=False, log_index=10))
    m.step(outbox[1].reply(1, reject=False, log_index=10))

    # These have gone up one because the leader has committed an empty log entry
    # As it has started a new term.
    assert m.peers["node2"].next_index == 5
    assert m.peers["node2"].match_index == 4


def test_append_entries_against_empty(event_loop):
    m = Machine("node1")
    m.add_peer("node2")
    m.add_peer("node3")

    m.tick = 0

    m.step(
        Msg(
            "node2",
            "node1",
            Message.AppendEntries,
            2,
            prev_index=0,
            prev_term=0,
            entries=[(2, {})],
            leader_commit=0,
        )
    )

    # Should reset election timeout
    assert m.tick > 0

    assert m.state == NodeState.FOLLOWER
    assert m.obedient is True
    assert m.leader == "node2"
    assert m.log[1:] == [(2, {})]
    assert m.commit_index == 0

    assert m.outbox[0].type == Message.AppendEntriesReply


def test_answer_pre_vote(event_loop):
    m = Machine("node1")
    m.add_peer("node2")
    m.add_peer("node3")
    m.term = 1

    # Vote rejected because in same term and obedient
    m.step(Msg("node2", "node1", Message.PreVote, 1, index=1))
    assert m.outbox[-1].type == Message.PreVoteReply
    assert m.outbox[-1].reject is True

    # Becomes a PRE_CANDIDATE - no longer obedient
    m.tick = 0
    m.step(Msg("node1", "node1", Message.Tick, 0))
    assert m.obedient is False
    assert m.state == NodeState.PRE_CANDIDATE
    assert m.term == 1

    # In a later term and not obedient
    m.tick = 0
    m.step(Msg("node2", "node1", Message.PreVote, 2, index=1))
    assert m.outbox[-1].type == Message.PreVoteReply
    assert m.outbox[-1].reject is False

    # Hasn't actually voted so this shouldn't be set
    assert m.voted_for is None


def test_answer_vote(event_loop):
    m = Machine("node1")
    m.add_peer("node2")
    m.add_peer("node3")
    m.term = 1

    # Vote rejected because in same term
    m.step(Msg("node2", "node1", Message.Vote, 1, index=1))
    assert m.outbox[-1].type == Message.VoteReply
    assert m.outbox[-1].reject is True

    # Vote in new term, but it is still obedient to current leader
    m.step(Msg("node2", "node1", Message.Vote, 2, index=1))
    assert m.outbox[-1].type == Message.VoteReply
    assert m.outbox[-1].reject is True

    # Becomes a PRE_CANDIDATE - nog longer obedient
    m.tick = 0
    m.step(Msg("node1", "node1", Message.Tick, 0))
    assert m.obedient is False
    assert m.state == NodeState.PRE_CANDIDATE
    assert m.term == 1

    # Vote in new term, but it is still obedient to current leader
    m.tick = 0
    m.step(Msg("node2", "node1", Message.Vote, 2, index=1))
    assert m.term == 2
    assert m.outbox[-1].type == Message.VoteReply
    assert m.outbox[-1].reject is False

    # Election timer reset after vote
    assert m.tick > 0

    # Pin to node until next reset
    assert m.voted_for == "node2"

    # Term should have increased
    assert m.term == 2


def test_append_entries_revoke_previous_log_entry(event_loop):
    m = Machine("node1")
    m.add_peer("node2")
    m.add_peer("node3")
    m.term = 2

    # Recovered from saved log
    m.log.append((1, {"type": "consensus"}))

    # Committed when became leader
    m.log.append((2, {}))

    m.step(
        Msg(
            "node2",
            "node1",
            Message.AppendEntries,
            term=3,
            prev_index=2,
            prev_term=3,
            entries=[],
            leader_commit=0,
        )
    )

    assert m.log[2] == (2, {})
    assert m.outbox[-1].type == Message.AppendEntriesReply
    assert m.outbox[-1].reject is True

    m.step(
        Msg(
            "node2",
            "node1",
            Message.AppendEntries,
            term=3,
            prev_index=1,
            prev_term=1,
            entries=[(3, {})],
            leader_commit=0,
        )
    )

    assert m.log[2] == (3, {})
    assert m.outbox[-1].type == Message.AppendEntriesReply
    assert m.outbox[-1].reject is False
    assert m.outbox[-1].log_index == 2


def test_find_inconsistencies(event_loop):
    n = Machine("node1")
    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None)], [(2, None), (2, None), (3, None)]
        )
        == 0
    )

    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None)], [(1, None), (2, None), (3, None)]
        )
        == 1
    )

    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None)], [(1, None), (1, None), (3, None)]
        )
        == 2
    )

    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None)], [(1, None), (1, None), (1, None)]
        )
        == 3
    )

    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None), (1, None)],
            [(1, None), (1, None), (1, None)],
        )
        == 3
    )

    assert (
        n.find_first_inconsistency(
            [(1, None), (1, None), (1, None)],
            [(1, None), (1, None), (1, None), (1, None)],
        )
        == 3
    )

*/
