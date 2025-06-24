# Installation

This is work in progress.

## Secrets

Right now our standard reference installation uses cert-manager to issue certificates and signing keypair.

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: token-issuer
  namespace: disty
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: signing-secret
  namespace: disty
spec:
  commonName: unused
  secretName: auth-signing
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: token-issuer
    kind: Issuer
    group: cert-manager.io
```

## Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: disty-config
  namespace: disty
data:
  config.yaml: |
    url: https://registry.example.com

    nodes:
    - id: 1
      addr_raft: registry-0.registry.disty.svc:6080
      addr_api: registry-0.registry.disty.svc:7080
      addr_registry: registry-0.registry.disty:8080
    - id: 2
      addr_raft: registry-1.registry.disty.svc:6080
      addr_api: registry-1.registry.disty.svc:7080
      addr_registry: registry-1.registry.disty:8080
    - id: 3
      addr_raft: registry-2.registry.disty.svc:6080
      addr_api: registry-2.registry.disty.svc:7080
      addr_registry: registry-2.registry.disty:8080

    raft:
      # FIXME: inject secret here
      secret: aaaaaaaaaaaaaaaa

    api:
      # FIXME: inject secret here
      secret: aaaaaaaaaaaaaaaa

    authentication:
      key_pair_file: /token-auth/tls.key

      users:
      - username: admin
        password: "$2b$10$drOwzBx3RMK3Q9Pk/88.Deh1.OdY5Vg0ex/wBwWh7.RP91HhNLFGG"

      acls:
      - subject:
          username: admin
        actions: ["push", "pull"]
        comment: "Admin has full access to everything."

    storage: /data
```

## The Cluster

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: registry
  namespace: disty
  annotations:
    configmap.reloader.stakater.com/reload: "disty-config"
spec:
  replicas: 3
  serviceName: registry

  selector:
    matchLabels:
      app: disty

  template:
    metadata:
      labels:
        app: disty

    spec:
      automountServiceAccountToken: false

      securityContext:
          fsGroup: 2001
          runAsUser: 2001
          runAsGroup: 2001

      nodeSelector:
        node-role.kubernetes.io/control-plane: ""
      volumes:
      - name: config
        configMap:
          name: disty-config
      - name: auth-verifying
        secret:
          secretName: auth-signing
      - name: data
        hostPath:
          path: /var/lib/storage/registry
          type: DirectoryOrCreate
      containers:
      - name: disty
        command:
        - /disty
        - -c
        - /config/config.yaml
        volumeMounts:
        - name: data
          mountPath: "/data"
        - name: config
          mountPath: "/config"
        - name: auth-verifying
          mountPath: "/token-auth"
        image: ghcr.io/unrouted/disty:main
        ports:
        - containerPort: 6080
        - containerPort: 7080
        - containerPort: 8080
        - containerPort: 9080
        env:
        - name: ENC_KEYS
          value: |
              q6u26onRvXVG4427/M0NFQzhSSldCY01rckJNa1JYZ3g2NUFtSnNOVGdoU0E=
              bVCyTsGaggVy5yqQ/UzluN29DZW41M3hTSkx6Y3NtZmRuQkR2TnJxUTYzcjQ=
        - name: ENC_KEY_ACTIVE
          value: bVCyTsGaggVy5yqQ
```

## Ingress

The registry is accessible on port 8080. Your ingress should point at that. No other port sould be exposed.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: disty
spec:
  sessionAffinity: ClientIP
  ports:
  - port: 8080
    protocol: TCP
    targetPort: 8080
  selector:
    app: disty
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  namespace: disty
  name: registry
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "500m"
    nginx.ingress.kubernetes.io/upstream-hash-by: $remote_addr
spec:
  tls:
  - hosts:
    - registry.example.com
    secretName: tls
  rules:
  - host: registry.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: registry
            port:
              number: 8080
```

## Monitoring

Prometheus is on port 9080.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: disty
  labels:
    app: disty
spec:
  ports:
  - name: prometheus
    protocol: TCP
    port: 9080
    targetPort: 9080
  selector:
    app: disty
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: disty
  namespace: disty
spec:
  endpoints:
  - interval: 15s
    port: prometheus
  namespaceSelector:
    matchNames:
    - disty
  selector:
    matchLabels:
      app: disty
```
