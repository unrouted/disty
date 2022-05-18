use log::{debug, warn};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::MachineMetricLabels;
use crate::{
    config::Configuration,
    log::{Log, LogEntry},
    types::RegistryAction,
};

const ELECTION_TICK_LOW: u64 = 150;
const ELECTION_TICK_HIGH: u64 = 300;
const HEARTBEAT_TICK: u64 = 100;

fn find_first_inconsistency(ours: Vec<LogEntry>, theirs: Vec<LogEntry>) -> usize {
    for (i, (our_entry, their_entry)) in ours.iter().zip(theirs.iter()).enumerate() {
        if our_entry.term != their_entry.term {
            return i;
        }
    }

    std::cmp::min(ours.len(), theirs.len())
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub struct Position {
    index: usize,
    term: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub enum Message {
    Tick {},
    Vote {
        index: usize,
    },
    VoteReply {
        reject: bool,
    },
    PreVote {
        index: usize,
    },
    PreVoteReply {
        reject: bool,
    },
    AppendEntries {
        leader_commit: Option<usize>,
        prev: Option<Position>,
        entries: Vec<LogEntry>,
    },
    AppendEntriesRejection {},
    AppendEntriesReply {
        log_index: Option<usize>,
    },
    AddEntries {
        entries: Vec<RegistryAction>,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Clone)]
pub struct Envelope {
    pub source: String,
    pub destination: String,
    pub term: usize,
    pub message: Message,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PeerState {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Peer {
    pub identifier: String,
    pub next_index: usize,
    pub match_index: usize,
    pub check_quorum_count: u32,
}
pub struct Machine {
    pub identifier: String,
    pub state: PeerState,
    pub term: usize,
    pub outbox: Vec<Envelope>,
    pub leader: Option<String>,
    peers: HashMap<String, Peer>,
    voted_for: Option<String>,
    pub tick: tokio::time::Instant,
    vote_count: usize,

    // The index we are about to flush to disk
    pub pending_index: usize,

    // The index we have flushed to disk
    pub stored_index: Option<usize>,

    // The index we know has been written to disk for at least the quorum
    pub commit_index: Option<usize>,

    // The index this node has applied to its state machine
    pub applied_index: usize,

    obedient: bool,

    last_committed: Family<MachineMetricLabels, Gauge>,
}

impl Machine {
    pub fn new(config: Configuration, registry: &mut Registry) -> Self {
        let mut peers = HashMap::new();
        for peer in config.peers {
            if peer.name != config.identifier {
                peers.insert(
                    peer.name.clone(),
                    Peer {
                        identifier: peer.name.clone(),
                        next_index: 0,
                        match_index: 0,
                        check_quorum_count: 0,
                    },
                );
            }
        }

        let last_committed = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_committed",
            "Last index that was committed",
            Box::new(last_committed.clone()),
        );

        Machine {
            identifier: config.identifier,
            state: PeerState::Follower,
            leader: None,
            term: 1,
            tick: tokio::time::Instant::now(),
            vote_count: 0,
            voted_for: None,
            obedient: true,
            outbox: vec![],
            pending_index: 0,
            stored_index: None,
            applied_index: 0,
            commit_index: None,
            peers,
            last_committed,
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

    fn reset(&mut self, term: usize) {
        if term != self.term {
            self.term = term;
            self.voted_for = None;
        }

        self.leader = None;
    }

    fn check_quorum(&mut self) {
        // Start at one because we count towards a quorum
        let mut count = 1;
        for peer in self.peers.values() {
            if peer.check_quorum_count > 500 {
                count += 1;
            }
        }

        if count >= self.quorum() {
            warn!("Machine: Could not reach majority of followers. Standing down.");
            self.become_follower(self.term, None);
        }
    }
    fn become_follower(&mut self, term: usize, leader: Option<String>) {
        debug!("Became follower of {leader:?}");
        self.state = PeerState::Follower;
        self.reset(term);
        self.leader = leader;
        self.reset_election_tick();
    }

    fn become_pre_candidate(&mut self, log: &mut Log) {
        debug!("Became pre-candidate {}", self.identifier);
        self.state = PeerState::PreCandidate;
        self.obedient = false;

        if self.quorum() == 1 {
            self.become_candidate(log);
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
                        index: self.stored_index.unwrap_or(0),
                    },
                    p,
                )
            })
            .collect();
        self.outbox.extend(messages);
    }

    fn become_candidate(&mut self, log: &mut Log) {
        debug!("Became candidate {}", self.identifier);
        self.state = PeerState::Candidate;
        self.reset(self.term + 1);
        self.vote_count = 1;
        self.voted_for = Some(self.identifier.clone());

        if self.quorum() == 1 {
            self.become_leader(log);
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
                        index: self.stored_index.unwrap_or(0),
                    },
                    p,
                )
            })
            .collect();
        self.outbox.extend(messages);
    }

    fn become_leader(&mut self, log: &mut Log) {
        debug!("Became leader {}", self.identifier);
        self.state = PeerState::Leader;
        self.reset(self.term);
        self.reset_election_tick();

        for peer in self.peers.values_mut() {
            peer.next_index = self.stored_index.unwrap_or(0);
            peer.match_index = 0;
        }

        self.append(log, RegistryAction::Empty);
        self.broadcast_entries(log);
    }

    fn append(&mut self, log: &mut Log, entry: RegistryAction) -> bool {
        match self.state {
            PeerState::Leader => {
                log.append(LogEntry {
                    term: self.term,
                    entry,
                });
                self.maybe_commit(log);
                true
            }
            _ => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, PeerState::Leader)
    }

    fn can_vote(&self, envelope: &Envelope) -> bool {
        if self.state == PeerState::Leader {
            debug!("Machine: Can't vote whilst leader");
            return false;
        }

        if self.obedient {
            debug!("Machine: Can't vote whilst obedient");
            return false;
        }

        let index = match envelope.message {
            Message::PreVote { index } => {
                if envelope.term > self.term {
                    return true;
                }

                index
            }
            Message::Vote { index } => index,
            _ => {
                debug!("Machine: Can only vote due to PreVote or Vote message");
                return false;
            }
        };

        if let Some(stored_index) = self.stored_index {
            if stored_index > index {
                debug!("Machine: We know more than them");
                return false;
            }
        }

        // We have already voted for this node
        match &self.voted_for {
            Some(voted_for) if voted_for == &envelope.source => {
                debug!("Machine: We already voted for them");
                return true;
            }
            _ => {}
        }

        // FIXME: Is last_term appropriate here???
        if let Message::PreVote { index: _ } = envelope.message {
            if envelope.term > self.term {
                debug!("Machine: Future term");
                return true;
            }
        }

        debug!("Machine: Default case");
        true
    }

    fn maybe_commit(&mut self, log: &mut Log) {
        if self.stored_index == self.commit_index {
            return;
        }

        let mut commit_index = None;
        let mut i = self.commit_index.unwrap_or(0);

        if let Some(stored_index) = self.stored_index {
            while i <= stored_index {
                if log.get(i).term != self.term {
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
                    commit_index = Some(i);
                }

                i += 1;
            }

            if commit_index <= self.commit_index {
                return;
            }

            self.commit_index = std::cmp::min(self.stored_index, commit_index);

            if let Some(commit_index) = self.commit_index {
                let labels = MachineMetricLabels {
                    identifier: self.identifier.clone(),
                };
                self.last_committed
                    .get_or_create(&labels)
                    .set(commit_index as u64);
            }
        }
    }

    pub fn step(&mut self, log: &mut Log, envelope: &Envelope) -> Result<(), String> {
        self.outbox.clear();

        log.truncate_index = None;

        self.stored_index = log.last_index();

        match envelope.message.clone() {
            Message::AddEntries { entries } => match self.state {
                PeerState::Leader => {
                    for entry in entries {
                        log.append(LogEntry {
                            term: self.term,
                            entry: entry.clone(),
                        });
                    }
                    self.maybe_commit(log);
                    self.broadcast_entries(log);
                }
                _ => {
                    return Err("Rejected: Not leader".to_string());
                }
            },
            Message::Vote { index: _ } => {

                if envelope.term < self.term {
                    debug!("Machine: Vote: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }

                if envelope.term <= self.term {
                    debug!("Vote for {} rejected", envelope.source);
                    self.reply(envelope, self.term, Message::VoteReply { reject: true });
                    return Ok(());
                }

                debug!("Become follower because vote");
                self.become_follower(envelope.term, None);

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

                if envelope.term < self.term {
                    debug!("Machine:VoteReply:  Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }

                if envelope.term > self.term {
                    self.become_follower(envelope.term, None);
                    return Ok(());
                }

                if self.state == PeerState::Candidate && !reject {
                    self.vote_count += 1;

                    if self.vote_count >= self.quorum() {
                        self.become_leader(log);
                    }
                }
            }

            Message::PreVote { index: _ } => {
                if envelope.term < self.term {
                    debug!("Machine: PreVote: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }

                if !self.can_vote(envelope) {
                    debug!("Vote for {} rejected", envelope.source);
                    self.reply(envelope, self.term, Message::PreVoteReply { reject: true });
                    return Ok(());
                }

                debug!("Will prevote for {}", envelope.source);
                self.reply(envelope, self.term, Message::PreVoteReply { reject: false });
            }

            Message::PreVoteReply { reject } => {
                if envelope.term < self.term {
                    debug!("Machine: PreVoteReply: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }

                if self.state == PeerState::PreCandidate && !reject {
                    self.vote_count += 1;

                    if self.vote_count >= self.quorum() {
                        self.become_candidate(log);
                    }
                }
            }

            Message::AppendEntries {
                leader_commit,
                prev,
                entries,
            } => {
                if envelope.term < self.term {
                    debug!("Machine: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }

                let entries = match prev {
                    None => {
                        if self.stored_index.is_some() {
                            debug!("Need to clear local state to recover");
                            log.truncate(0);
                        }

                        entries
                    }

                    Some(Position { index, term }) => match self.stored_index {
                        None => entries,
                        Some(stored_index) => {
                            if index > stored_index {
                                debug!("Leader assumed we had log entry {index} but we do not");
                                self.reply(
                                    envelope,
                                    envelope.term,
                                    Message::AppendEntriesRejection {},
                                );
                                return Ok(());
                            }

                            if log.get(index).term != term {
                                debug!(
                                    "Log not valid - mismatched terms: {} vs {}",
                                    log.get(index).term,
                                    term
                                );
                                self.reply(
                                    envelope,
                                    envelope.term,
                                    Message::AppendEntriesRejection {},
                                );
                                return Ok(());
                            }

                            let offset = find_first_inconsistency(
                                log.entries[index..].to_vec(),
                                entries.clone(),
                            );
                            let prev_index = index + offset;

                            if stored_index > prev_index {
                                warn!("Need to truncate log to recover quorum");
                                log.truncate(prev_index);
                            }

                            entries[offset..].to_vec()
                        }
                    },
                };

                match self.state {
                    PeerState::Follower => {
                        self.reset_election_tick();
                        self.term = envelope.term;
                    }
                    _ => {
                        self.become_follower(envelope.term, Some(envelope.source.clone()));
                    }
                }

                self.obedient = true;
                self.leader = Some(envelope.source.clone());

                for entry in entries {
                    log.append(entry.clone());
                }

                // FIXME: As it stands stored_index won't advance immediately
                // So log_index will lag, and the master will re-send stuff maybe?
                // Investigate whether next_index and match_index help here

                // If we have some stored commits and the leader has commmitted some stuff
                // Advance self.commit_index
                if leader_commit > self.commit_index {
                    self.commit_index = std::cmp::min(leader_commit, self.stored_index);
                }

                self.reply(
                    envelope,
                    self.term,
                    Message::AppendEntriesReply {
                        log_index: self.stored_index,
                    },
                );
            }
            Message::AppendEntriesReply { log_index } => {
                if envelope.term < self.term {
                    debug!("Machine: AppendEntriesReply: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }
                if envelope.term > self.term {
                    self.become_follower(envelope.term, None);
                }
                if matches!(self.state, PeerState::Leader) {
                    let mut peer = self.peers.get_mut(&envelope.source).unwrap();
                    peer.check_quorum_count = 0;
                    peer.match_index =
                        std::cmp::min(log_index.unwrap(), self.stored_index.unwrap());
                    peer.next_index = peer.match_index + 1;
                    self.maybe_commit(log);
                    return Ok(());
                }
            }
            Message::AppendEntriesRejection {} => {
                if envelope.term < self.term {
                    debug!("Machine: AppendEntriesRejection: Dropping message from old term: {} {}", envelope.term, self.term);
                    return Ok(());
                }
                if envelope.term > self.term {
                    self.become_follower(envelope.term, None);
                }
                if matches!(self.state, PeerState::Leader) {
                    let mut peer = self.peers.get_mut(&envelope.source).unwrap();
                    peer.check_quorum_count = 0;
                    if peer.next_index > 1 {
                        peer.next_index -= 1;
                    }
                    return Ok(());
                }
            }
            Message::Tick {} => {
                match self.state {
                    PeerState::Leader => {
                        self.broadcast_entries(log);
                        self.check_quorum();
                    }
                    PeerState::Follower => {
                        // Heartbeat timeout - time to start thinking about elections
                        self.become_pre_candidate(log)
                    }
                    PeerState::PreCandidate => {
                        // Pre-election timed out before receiving all votes
                        debug!("PreCandidate tick");
                        self.become_follower(self.term, None);
                    }
                    PeerState::Candidate => {
                        // Election timed out before receiving all votes
                        self.become_follower(self.term, None);
                    }
                }
            }
        }

        Ok(())
    }

    fn envelope(&self, term: usize, message: Message, peer: &Peer) -> Envelope {
        Envelope {
            source: self.identifier.clone(),
            destination: peer.identifier.clone(),
            term,
            message,
        }
    }

    fn broadcast_entries(&mut self, log: &mut Log) {
        let mut messages: Vec<Envelope> = vec![];

        for peer in self.peers.values_mut() {
            let message = match self.stored_index {
                None => Message::AppendEntries {
                    prev: None,
                    entries: log.entries.clone(),
                    leader_commit: self.commit_index,
                },
                Some(stored_index) => {
                    let index = std::cmp::max(std::cmp::min(peer.next_index, stored_index), 0);
                    let term = log.get(index).term;

                    Message::AppendEntries {
                        prev: Some(Position { index, term }),
                        entries: log.entries[index..].to_vec(),
                        leader_commit: self.commit_index,
                    }
                }
            };

            messages.push(Envelope {
                source: self.identifier.clone(),
                destination: peer.identifier.clone(),
                term: self.term,
                message,
            });

            peer.check_quorum_count += 1;
        }
        self.outbox.extend(messages);

        self.maybe_commit(log);

        self.reset_heartbeat_tick();
    }

    fn reply(&mut self, envelope: &Envelope, term: usize, message: Message) {
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
    use chrono::Utc;

    use crate::{
        config::{PeerConfig, RaftConfig, RegistryConfig},
        types::{Digest, RepositoryName},
    };

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
        let mut log = Log::default();
        let mut m = single_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Leader);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn cluster_node_become_pre_candidate() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
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
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // Next tick occurs after voting period times out

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Follower);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn pre_vote_reject_old_term() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 1,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);

        // A single prevote lets us become a candidate

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
    }

    #[test]
    fn cluster_node_become_candidate() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: m.term,
            },
        )
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
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: m.term,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);
        assert_eq!(m.outbox.len(), 2);

        // But a tick before enough votes means we stay a follower

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Follower);
        assert_eq!(m.outbox.len(), 0);
    }

    #[test]
    fn cluster_node_become_leader() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.outbox.len(), 2);

        // A single prevote lets us become a candidate

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: m.term,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);
        assert_eq!(m.outbox.len(), 2);

        // A single vote lets us become a leader

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::VoteReply { reject: false },
                term: 2,
            },
        )
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
                        leader_commit: None,
                        prev: None,
                        entries: vec![LogEntry {
                            term: 2,
                            entry: RegistryAction::Empty
                        }]
                    }
                },
                Envelope {
                    source: "node1".to_string(),
                    destination: "node3".to_string(),
                    term: 2,
                    message: Message::AppendEntries {
                        leader_commit: None,
                        prev: None,
                        entries: vec![LogEntry {
                            term: 2,
                            entry: RegistryAction::Empty
                        }]
                    }
                }
            ]
        );
    }

    #[test]
    fn check_quorum_stand_down() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);

        // A single prevote lets us become a candidate

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: 2,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);

        // A single vote lets us become a leader

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::VoteReply { reject: false },
                term: 2,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Leader);

        for _ in 1..501 {
            assert_eq!(m.state, PeerState::Leader);

            m.step(
                &mut log,
                &Envelope {
                    source: "node2".to_string(),
                    destination: "node1".to_string(),
                    message: Message::Tick {},
                    term: 0,
                },
            )
            .unwrap();
        }

        assert_eq!(m.state, PeerState::Follower);
    }

    #[test]
    fn leader_handle_append_entries_reply_success() {
        let mut log = Log::default();

        log.entries.extend(vec![LogEntry {
            term: 1,
            entry: RegistryAction::Empty,
        }]);
        log.entries.extend(vec![LogEntry {
            term: 1,
            entry: RegistryAction::Empty,
        }]);
        log.entries.extend(vec![LogEntry {
            term: 1,
            entry: RegistryAction::Empty,
        }]);

        let mut m = cluster_node_machine();
        m.term = 1;

        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::PreCandidate);

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVoteReply { reject: false },
                term: m.term,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Candidate);

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::VoteReply { reject: false },
                term: m.term,
            },
        )
        .unwrap();

        assert_eq!(m.state, PeerState::Leader);

        m.outbox.sort();

        assert_eq!(
            m.outbox,
            vec![
                Envelope {
                    source: "node1".to_string(),
                    destination: "node2".to_string(),
                    term: 2,
                    message: Message::AppendEntries {
                        leader_commit: None,
                        prev: Some(Position { index: 2, term: 1 }),

                        entries: vec![
                            LogEntry {
                                term: 1,
                                entry: RegistryAction::Empty,
                            },
                            LogEntry {
                                term: 2,
                                entry: RegistryAction::Empty,
                            }
                        ]
                    }
                },
                Envelope {
                    source: "node1".to_string(),
                    destination: "node3".to_string(),
                    term: 2,
                    message: Message::AppendEntries {
                        leader_commit: None,
                        prev: Some(Position { index: 2, term: 1 }),

                        entries: vec![
                            LogEntry {
                                term: 1,
                                entry: RegistryAction::Empty,
                            },
                            LogEntry {
                                term: 2,
                                entry: RegistryAction::Empty,
                            }
                        ]
                    }
                }
            ]
        );

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::AppendEntriesReply { log_index: Some(4) },
                term: 2,
            },
        )
        .unwrap();

        m.step(
            &mut log,
            &Envelope {
                source: "node3".to_string(),
                destination: "node1".to_string(),
                message: Message::AppendEntriesReply { log_index: Some(4) },
                term: 2,
            },
        )
        .unwrap();

        let mut peers = m.peers.values().cloned().collect::<Vec<Peer>>();
        peers.sort();

        assert_eq!(
            peers,
            vec![
                Peer {
                    identifier: "node2".to_string(),
                    next_index: 4,
                    match_index: 3,
                    check_quorum_count: 0,
                },
                Peer {
                    identifier: "node3".to_string(),
                    next_index: 4,
                    match_index: 3,
                    check_quorum_count: 0,
                }
            ]
        );
    }

    #[test]
    fn append_entries_against_empty() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::AppendEntries {
                    leader_commit: Some(0),
                    prev: Some(Position { index: 0, term: 0 }),
                    entries: vec![LogEntry {
                        term: 2,
                        entry: RegistryAction::Empty,
                    }],
                },
                term: 2,
            },
        )
        .unwrap();

        assert!(m.tick > tokio::time::Instant::now());
        assert_eq!(m.state, PeerState::Follower);
        // assert_eq!(m.leader, Some("node2".to_string()));
        assert_eq!(
            log.entries,
            vec![LogEntry {
                term: 2,
                entry: RegistryAction::Empty,
            }]
        );
        assert_eq!(m.commit_index, None);

        assert_eq!(m.outbox.len(), 1);
        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 2,
                message: Message::AppendEntriesReply { log_index: None }
            },]
        );
    }

    #[test]
    fn answer_pre_vote() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.term = 1;

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVote { index: 1 },
                term: 2,
            },
        )
        .unwrap();

        // Vote rejected because in same term and obedient
        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 1,
                message: Message::PreVoteReply { reject: true }
            },]
        );

        // Becomes a PRE_CANDIDATE - no longer obedient
        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert!(!m.obedient);
        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.term, 1);

        //  In a later term and not obedient
        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::PreVote { index: 1 },
                term: 2,
            },
        )
        .unwrap();

        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 1,
                message: Message::PreVoteReply { reject: false }
            },]
        );

        // Hasn't actually voted so this shouldn't be set
        assert_eq!(m.voted_for, None);
    }

    #[test]
    fn answer_vote() {
        let mut log = Log::default();
        let mut m = cluster_node_machine();

        m.term = 1;

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::Vote { index: 1 },
                term: 2,
            },
        )
        .unwrap();

        // Vote rejected because in same term and obedient
        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 2,
                message: Message::VoteReply { reject: true }
            },]
        );

        // Becomes a PRE_CANDIDATE - no longer obedient
        m.step(
            &mut log,
            &Envelope {
                source: "node1".to_string(),
                destination: "node1".to_string(),
                message: Message::Tick {},
                term: 0,
            },
        )
        .unwrap();

        assert!(!m.obedient);
        assert_eq!(m.state, PeerState::PreCandidate);
        assert_eq!(m.term, 2);

        // Not obedient
        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::Vote { index: 1 },
                term: 2,
            },
        )
        .unwrap();

        // Bu vote in old term denied
        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 2,
                message: Message::VoteReply { reject: true }
            },]
        );

        // Not obedient and vote in new term
        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::Vote { index: 1 },
                term: 3,
            },
        )
        .unwrap();

        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 3,
                message: Message::VoteReply { reject: false }
            },]
        );

        // Hasn't actually voted so this shouldn't be set
        assert_eq!(m.voted_for, Some("node2".to_string()));
    }

    #[test]
    fn append_entries_revoke_previous_log_entry() {
        let timestamp = Utc::now();

        let mut log = Log::default();

        // Recovered from saved log
        log.entries.extend(vec![LogEntry {
            term: 1,
            entry: RegistryAction::BlobMounted {
                timestamp,
                digest: "sha256:1234".parse().unwrap(),
                repository: "foo".parse().unwrap(),
                user: "test".to_string(),
            },
        }]);

        // Committed when became leader
        log.entries.extend(vec![LogEntry {
            term: 2,
            entry: RegistryAction::Empty,
        }]);

        let mut m = cluster_node_machine();
        m.term = 2;

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::AppendEntries {
                    leader_commit: Some(0),
                    prev: Some(Position { index: 1, term: 3 }),
                    entries: vec![],
                },
                term: 3,
            },
        )
        .unwrap();

        assert_eq!(
            log.entries[1],
            LogEntry {
                term: 2,
                entry: RegistryAction::Empty
            }
        );

        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 3,
                message: Message::AppendEntriesRejection {}
            },]
        );

        m.step(
            &mut log,
            &Envelope {
                source: "node2".to_string(),
                destination: "node1".to_string(),
                message: Message::AppendEntries {
                    leader_commit: Some(0),
                    prev: Some(Position { index: 1, term: 2 }),
                    entries: vec![],
                },
                term: 3,
            },
        )
        .unwrap();

        assert_eq!(
            m.outbox,
            vec![Envelope {
                source: "node1".to_string(),
                destination: "node2".to_string(),
                term: 3,
                message: Message::AppendEntriesReply { log_index: Some(1) }
            },]
        );

        assert_eq!(
            log.entries,
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::BlobMounted {
                        timestamp,
                        digest: Digest {
                            algo: "sha256".to_string(),
                            hash: "1234".to_string()
                        },
                        repository: RepositoryName {
                            name: "foo".to_string()
                        },
                        user: "test".to_string()
                    }
                },
                LogEntry {
                    term: 2,
                    entry: RegistryAction::Empty
                }
            ]
        );
    }

    #[test]
    fn find_inconsistencies() {
        let n = find_first_inconsistency(
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
            vec![
                LogEntry {
                    term: 2,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 2,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 3,
                    entry: RegistryAction::Empty,
                },
            ],
        );

        assert_eq!(n, 0);

        let n = find_first_inconsistency(
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 2,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 3,
                    entry: RegistryAction::Empty,
                },
            ],
        );

        assert_eq!(n, 1);

        let n = find_first_inconsistency(
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 3,
                    entry: RegistryAction::Empty,
                },
            ],
        );

        assert_eq!(n, 2);

        let n = find_first_inconsistency(
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
        );

        assert_eq!(n, 3);

        let n = find_first_inconsistency(
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
            vec![
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
                LogEntry {
                    term: 1,
                    entry: RegistryAction::Empty,
                },
            ],
        );

        assert_eq!(n, 3);
    }
}
