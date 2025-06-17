# disty

**disty** is a lightweight, distributed container image registry designed for in-cluster, fault-tolerant deployments. Built in [Rust](https://www.rust-lang.org/) using [Axum](https://docs.rs/axum/), [OpenRaft](https://databendlabs.github.io/openraft/), and [Hiqlite](https://github.com/sebadob/hiqlite), disty aims to be a self-contained registry for Kubernetes-native environments where an out-of-cluster registry just doesn't cut it.

---

## ✨ Features

- 🐳 **Standards Compatible**: Push and pull images using standard tools like `docker`, `podman`, or `skopeo`
- 📦 **Distributed**: Uses Raft consensus to replicate container metadata across all nodes. Each node ensures it has all the objects in the metadata store locally.  
- 💾 **No special storage**: Doesn't need an S3 compatible object store or a kubernetes native storage driver to provide redundancy or resilience.
- 💡 **Minimal Dependencies**: No need for external databases
- 🧱 **Kubernetes-Ready**: Simple to deploy as a native part of your Kubernetes cluster

---

## 🔧 Why disty?

An image registry is often at that heart of your architecture. The scenarios where **disty** might be exactly what you need are:

- 🛠 You **must self-host** and cannot use external or cloud-based image registries
- 🏗 You want to **run entirely in-cluster** without dedicating extra physical or virtual machines to the registry
- 🧩 You require **fault tolerance** without the complexity of maintaining separate HA storage systems

---

## 🚧 How does it work?

* We use local storage on the node to store manifests and blobs.
* Raft is used to maintain a fault toleratant and distributed database of all manifests and blobs we are tracking.
* The nodes essentially "docker pull" between themselves until all the object stores are in sync. This is orchestrated throught the raft database.

---

## 🤝 Contributing

Contributions are welcome! Please file issues, suggest improvements, or submit PRs. Be aware this is an early-stage project and subject to change.

---

## 📜 License

This project is licensed under the [Apache License 2.0](LICENSE).

---

## 📫 Contact

Have questions or ideas? Open an issue or reach out via the Discussions tab.
