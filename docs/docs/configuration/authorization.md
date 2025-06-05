By default authenticated users cannot push or pull to any repository. You need to write acl's to let them do anything.

Rules match against the subject (the actor making a request) and the resource (the repository). Rules that match add to the set of permitted actions.

## Authenticating the subject

### Username

```yaml
authentication:
  acls:
  - subject:
      username: admin
    actions: [push, pull]
    comment: Admin can do what they like
```

### IP Address

Basic auth and ID Token authorization can restrict token issuance by IP address:

```yaml
authentication:
  acls:
  - subject:
      network: 192.168.0.0/24
    actions: [push, pull]
    comment: Local users can do what they like
```

### ID Token Claims

JTW tokens (such as those issued by GitLab during CI) have claims that provide more context about the intention of the token. For example, a GitLab ID Token contains a lot of information about the CI task where it was issued:

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

This is really powerful - we can authorize against not just the identity of the git server but the context of the operation - the branch, the deployment environment, or whether its a tag or a branch.

```yaml
authentication:
  acls:
    - subject:
        claims:
          environment: production
          namespace_path: { regex: ^mynamespace/.*$ }
      resource:
        repository: { regex: ^mynamespace/.*$ }
      actions: [push, pull]
      comment: Product builds in mynamespace can do what they like to mynamespace
```

## Authenticating the resource

### Repository name

```yaml
authentication:
  acls:
    - resource:
        repository: { regex: ^free4all/.*$ }
      actions: [push, pull]
      comment: Any authenticated user can do what they want under `free4all/`
```