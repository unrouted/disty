use prometheus_client::encoding::text::Encode;

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
pub struct MachineMetricLabels {
    pub identifier: String,
}
