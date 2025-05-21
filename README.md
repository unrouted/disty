# distry

**distry** is a lightweight, distributed container image registry designed for in-cluster, fault-tolerant deployments. Built in [Rust](https://www.rust-lang.org/) using [Axum](https://docs.rs/axum/), [OpenRaft](https://databendlabs.github.io/openraft/), and [Hiqlite](https://github.com/sebadob/hiqlite), distry aims to be a self-contained solution for Kubernetes-native environments where traditional cloud or centralized registries aren't a viable option.

---

## ✨ Features

- 🐳 **Docker-Compatible**: Push and pull images using standard tools like `docker`, `podman`, or `skopeo`
- 📦 **Distributed Metadata**: Uses Raft consensus to replicate container **metadata** across all nodes  
- 💾 **Local Image Storage**: Actual image data (blobs) is stored locally on each node
- 💡 **Minimal Dependencies**: No need for external databases or shared storage layers
- 🧱 **Kubernetes-Ready**: Simple to deploy as a native part of your Kubernetes cluster
- 🦀 **Rust-Powered**: Modern, efficient, and safe implementation with future-facing architecture

---

## 🔧 Why distry?

There are a very specific set of scenarios where **distry** might be exactly what you need:

- 🛠 You **must self-host** and cannot use external or cloud-based image registries  
- 🏗 You want to **run entirely in-cluster** without dedicating extra machines to Harbor, Quay, or other solutions  
- 🧩 You require **fault tolerance** without the complexity of maintaining separate HA storage systems  

---

## 🚧 Project Status

> **distry is a work in progress.**  
> It was originally written in Python, then rewritten in Rust, and later migrated to use OpenRaft. It is now being redesigned with Hiqlite for embedded, distributed persistence.

Currently, Raft is used for replicating image metadata (e.g., manifests, tags) across nodes. Image blobs are stored locally and are not replicated by the registry itself.

---

## 🤝 Contributing

Contributions are welcome! Please file issues, suggest improvements, or submit PRs. Be aware this is an early-stage project and subject to change.

---

## 📜 License

This project is licensed under the [Apache License 2.0](LICENSE).

---

## 📫 Contact

Have questions or ideas? Open an issue or reach out via the Discussions tab.
