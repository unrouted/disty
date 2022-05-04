use prometheus_client::encoding::text::Encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
struct MachineMetricLabels {
    identifier: String,
}
pub struct Machine {
    pub identifier: String,

    last_applied_index: Family<MachineMetricLabels, Gauge>,
    last_committed: Family<MachineMetricLabels, Gauge>,
    last_saved: Family<MachineMetricLabels, Gauge>,
    last_term_saved: Family<MachineMetricLabels, Gauge>,
    current_term: Family<MachineMetricLabels, Gauge>,
    current_state: Family<MachineMetricLabels, Gauge>,
}

impl Machine {
    pub fn new(registry: &mut Registry, identifier: String) -> Self {
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

        Machine {
            identifier,
            last_applied_index,
            last_committed,
            last_saved,
            last_term_saved,
            current_term,
            current_state,
        }
    }
}
