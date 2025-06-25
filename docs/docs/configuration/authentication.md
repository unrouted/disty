Docker and Docker compatible tools typically do basic auth to get a JWT token that your registry server trusts. THis is time limited and scoped to a repository.

## Basic Authentication

The issuer can be configured with a list of username/password pairs:

```yaml
authentication:
  users:
  - username: john
    password: $1$241fa867$Xz9XpMlkGLq2lO/PmIdGY0
```

## ID Token Authentication

Some systems (such as GitLab) can issue JWT tokens. The CI task can set your registry server as the audience, and we can verify that it is signed by an active key by checking well known URL's on the issuer. This is really powerful because these tokens are short lived.

So that these can be used with off the shelf tools like kaniko, docker and podman we can read the token out of a basic auth request (the token is treated as a password).

For GitLab, this looks like this:

```yaml
authentication:
  users:
  - username: gitlab
    issuer:
      issuer: gitlab.example.com
      jwks_url: https://gitlab.example.com/oauth/discovery/keys
```

For Kubernetes, it looks like this:

```yaml
authentication:
  users:
  - username: kubelet
    issuer:
      issuer: kubernetes/serviceaccount
      jwks_url: https://kubernetes.default.svc/openid/v1/jwks
      bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
```
