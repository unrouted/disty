By default, authenticated users cannot push or pull to any repository. You must write ACLs to grant permissions.

Access rules are defined under:

```yaml
authentication:
  acls:
    - resource: ...
      subject: ...
      actions: [push, pull]
```

Each ACL matches:

* `resource`: What is being accessed
* `subject`: Who is making the request

And grants:

* `actions`: What they can do. Must be `push` or `pull`.

ACLs are additive - matching any ACL can grant permission.

## Matching the resource

You should restrict access to specific repositories.

### Repository name

Allow `bob` to push/pull to `bob/webapp`

```yaml
authentication:
  acls:
    - resource:
        repository: bob/webapp
      subject:
        username: bob
      actions: [push, pull]
```

Allow any user to push/pull to repositories under `free4all/`:

```yaml
authentication:
  acls:
    - resource:
        repository: { regex: ^free4all/.*$ }
      actions: [push, pull]
```

## Matching the subject

The subject is the identity making the request, e.g. username, network, or claims in an ID token.

### Username

Allow a specific user to do anything:

```yaml
authentication:
  acls:
    - subject:
        username: admin
      actions: [push, pull]
```

### Network (IP/CIDR)

Allow users from the local network to do anything:

```yaml
authentication:
  acls:
    - subject:
        network: 192.168.0.0/24
      actions: [push, pull]
```

### ID Token claims

If your users authenticate via JWT / ID Token, you can match fields in the claims.

For example, this allows you to harden your image respository to refuse pushes from CI jobs that are running in pull requests or not against the production environment.

#### JSON Pointer

JSON Pointer defines a string syntax for identifying a specific value within a JSON document. Given:

```json
{
  "a": {
    "b": {
      "c": 1
    }
  }
}
```

You can use the expression `/a/b/c` to find the value of `c`.

This example limits builds to running in the production environment:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - pointer: /environment
            match: production
      actions: [push, pull]
```

Here:

* `pointer` is the JSON Pointer to extract the value. `/` should be escaped as `~1`. `~` is escaped as `~0`.
* `match` is a string, number or ip matcher. You can use and, or and not matches to make richer matching rules.

#### JSON Path

You can also match values deeper in arrays using JSONPath.

For example, you can match if user belongs to a group starting with admin-:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - path: $.groups_direct[?(@ =~ /^admin-.*/)]
            exists: true
      actions: [push, pull]
```

Here:

* `path` is a JSONPath query that extracts values from a key called `groups_direct`.
* `exists: true` means the rule matches if any value exists

#### Example integrations

##### GitLab

A GitLab job can get a JWT with the following .gitlab-ci.yml:

```yaml
job_with_id_tokens:
  id_tokens:
    MY_ID_TOKEN:
      aud: https://registry.example.com
  script:
    - log-in-to-docker.sh kubernetes:$MY_ID_TOKEN
```

A GitLab JWT looks like this:

```json
{
  "namespace_id": "72",
  "namespace_path": "my-group",
  "project_id": "20",
  "project_path": "my-group/my-project",
  "user_id": "1",
  "user_login": "sample-user",
  "user_email": "sample-user@example.com",
  "user_identities": [
      {"provider": "github", "extern_uid": "2435223452345"},
      {"provider": "bitbucket", "extern_uid": "john.smith"}
  ],
  "pipeline_id": "574",
  "pipeline_source": "push",
  "job_id": "302",
  "ref": "feature-branch-1",
  "ref_type": "branch",
  "ref_path": "refs/heads/feature-branch-1",
  "ref_protected": "false",
  "groups_direct": ["mygroup/mysubgroup", "myothergroup/myothersubgroup"],
  "environment": "test-environment2",
  "environment_protected": "false",
  "deployment_tier": "testing",
  "environment_action": "start",
  "runner_id": 1,
  "runner_environment": "self-hosted",
  "sha": "714a629c0b401fdce83e847fc9589983fc6f46bc",
  "project_visibility": "public",
  "ci_config_ref_uri": "gitlab.example.com/my-group/my-project//.gitlab-ci.yml@refs/heads/main",
  "ci_config_sha": "714a629c0b401fdce83e847fc9589983fc6f46bc",
}
```

You can limit builds to your `main` branch and your `production` environment:

```yaml
authentication:
  acls:
    - repository:
        name: production/mywebapp
      subject:
        claims:
          - pointer: /environment
            match: production
          - pointer: /ref_type
            match: branch
          - pointer: /ref
            match: main
      actions: [push, pull]
```

You can do regexes. Here any branch starting `testing-` for the `dev` environment can push to the repository:

```yaml
authentication:
  acls:
    - repository:
        name: staging/mywebapp
      subject:
        claims:
          - pointer: /environment
            match: dev
          - pointer: /ref_type
            match: branch
          - pointer: /ref
            match:
              regex: ^testing-.*$
      actions: [push, pull]
```

You can combine different matchers. Here you can build for `production` from `main` and you can build for `dev` from a `testing-` branch:

```yaml
authentication:
  acls:
    - repository:
        name: mywebapp
      subject:
        claims:
          - or:
            - and:
              - pointer: /environment
                match: production
              - pointer: /ref_type
                match: branch
              - pointer: /ref
                match: main
            - and:
              - pointer: /environment
                match: dev
              - pointer: /ref_type
                match: branch
              - pointer: /ref
                match:
                  regex: ^testing-.*$
      actions: [push, pull]
```

##### Kubernetes

A kubernetes JWT looks like this:

```json
{
  "kubernetes.io": {
    "namespace": "my-namespace",
    "node": {
      "name": "my-node",
      "uid": "646e7c5e-32d6-4d42-9dbd-e504e6cbe6b1"
    },
    "pod": {
      "name": "my-pod-12345",
      "uid": "5e0bd49b-f040-43b0-99b7-22765a53f7f3"
    },
    "serviceaccount": {
      "name": "my-serviceaccount",
      "uid": "14ee3fa4-a7e2-420f-9f9a-dbc4507c3798"
    }
  }
}
```

You can then write a matcher for namespace `my-namespace` and deployment `my-pod` like this:

```yaml
authentication:
  acls:
    - repository:
        name: mywebapp
      subject:
        claims:
          - pointer: /kubernetes.io/namespace
            match: my-namespace
          - pointer: /kubernetes.io/pod/name
            match:
              regex: ^my-pod-.*$
      actions: [pull]
```

(This makes a bit of an assumption that you can adequately lock down your cluster so that `my-pod-*` was created by a `my-pod` deployment resource. You might need additional infrastructure for stronger assurances).

## Syntax reference

### Matcher data types

Matchers can handle different data types:

#### String

```yaml
match: somevalue
match: { regex: ^prod- }
```

#### Number

```yaml
match: 42
```

#### IP address / CIDR

```yaml
match: 10.0.0.0/8
```

### Existence of a value

Check if a JSON Path query finds anything:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - path: $.groups_direct[?(@ =~ /^admins-/)]
            exists: true
      actions: [push, pull]
```

### Combining matchers

ACLs can become very expressive by combining multiple claim matchers.

For example, you can allow any RFC1918 address except 192.168.1.1:

```yaml
authentication:
  acls:
    - subject:
        and:
          - or:
              - 10.0.0.0/8
              - 172.16.0.0/12
              - 192.168.0.0/16
          - not:
              network: 192.168.1.1
      actions: [push, pull]
```

### or: match if any matcher is true

Example: allow either production or dev environments:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - or:
              - pointer: /environment
                match: production
              - pointer: /environment
                match: dev
      actions: [push, pull]
```

### and: match if all conditions are true

Instead of directly under subject, combine multiple claims.

Example: must be in production and namespace starts with secure-:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - pointer: /environment
            match: production
          - pointer: /namespace_path
            match: { regex: ^secure- }
      actions: [push, pull]
```

All claims must match.

### not: invert a matcher

Example: block a specific environment:

```yaml
authentication:
  acls:
    - subject:
        claims:
          - not:
              pointer: /environment
              match: staging
      actions: [push, pull]
```
