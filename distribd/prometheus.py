from prometheus_client.core import GaugeMetricFamily


class MetricsCollector:
    def __init__(self, raft):
        self.raft = raft
        self.machine = raft.machine
        self.identifier = self.machine.identifier
        self.reducers = raft.reducers

    def collect(self):
        last_applied = GaugeMetricFamily(
            "distribd_last_applied_index",
            "Last index that was applied",
            labels=["identifier"],
        )
        last_applied.add_metric([self.identifier], self.reducers.applied_index)
        yield last_applied

        last_committed = GaugeMetricFamily(
            "distribd_last_committed_index",
            "Last index that was committed",
            labels=["identifier"],
        )
        last_committed.add_metric([self.identifier], self.machine.commit_index)
        yield last_committed

        last_saved = GaugeMetricFamily(
            "distribd_last_saved_index",
            "Last index that was stored in the commit log",
            labels=["identifier"],
        )
        last_saved.add_metric([self.identifier], self.machine.log.last_index)
        yield last_saved

        last_term_saved = GaugeMetricFamily(
            "distribd_last_saved_term",
            "Last term that was stored in the commit log",
            labels=["identifier"],
        )
        last_term_saved.add_metric([self.identifier], self.machine.log.last_term)
        yield last_term_saved

        current_term = GaugeMetricFamily(
            "distribd_current_term",
            "The current term for a node",
            labels=["identifier"],
        )
        current_term.add_metric([self.identifier], self.machine.term)
        yield current_term

        current_state = GaugeMetricFamily(
            "distribd_current_state",
            "The current state for a node",
            labels=["identifier", "state"],
        )
        current_state.add_metric([self.identifier, str(self.machine.state)], 1)
        yield current_state
