# Release Process

`axctl` releases are designed for main-branch integration and automatic PyPI
publication.

## Flow

1. Branch off `main` and open a PR against `main`.
2. Validate with automated tests, package build when needed, and
   the operator QA sequence in [Operator QA Runbook](./operator-qa-runbook.md).
3. Merge the PR into `main` after review.
4. Release Please opens or updates a release PR on `main` with:
   - `pyproject.toml` version bump
   - `.release-please-manifest.json` version bump
   - `CHANGELOG.md` entries generated from conventional commits
5. Merge the Release Please PR when the changelog/version are acceptable.
6. The GitHub release publication triggers the PyPI publish workflow.
7. PyPI publishes the package version. If that version already exists, publish
   is skipped.

## Required Operator QA Sequence

Before MCP Jam, widget, Playwright, release, or production-facing promotion
work moves forward, run the sequence below:

1. `axctl auth doctor` explains identity and config resolution.
2. `axctl qa preflight` is the required single-environment API gate.
3. `axctl qa matrix` compares environment drift across dev, next, prod, or
   customer targets.
4. Only after those pass should MCP Jam, widget, Playwright, or manual release
   QA continue.

Copy-paste dev gate:

```bash
axctl auth doctor --env dev --space-id <dev-space-id> --json
axctl qa preflight --env dev --space-id <dev-space-id> --for release --artifact .ax/qa/dev-preflight.json --json
```

Copy-paste next gate:

```bash
axctl auth doctor --env next --space-id <next-space-id> --json
axctl qa preflight --env next --space-id <next-space-id> --for release --artifact .ax/qa/next-preflight.json --json
```

Copy-paste promotion drift check:

```bash
axctl qa matrix \
  --env dev \
  --env next \
  --space dev=<dev-space-id> \
  --space next=<next-space-id> \
  --for release \
  --artifact-dir .ax/qa/promotion \
  --json
```

Attach or summarize the generated artifacts in the promotion PR when the change
touches auth, profiles, messages, uploads, listeners, MCP, UI validation, or
release behavior.

CI also calls the reusable `.github/workflows/operator-qa.yml` workflow for PRs
targeting `main`. It skips safely when no complete QA environment variables and
secrets are configured, uploads artifacts when it runs, and fails the promotion
path when a configured matrix returns `ok: false`.

The operator QA commands use stable exit codes: `0` for pass, `2` for a failed
gate, `3` for skipped/no config, and `1` for crashes or command usage failures.

## Commit Conventions

Use Conventional Commit prefixes so Release Please can choose the version bump:

- `fix:` creates a patch release.
- `feat:` creates a minor release.
- `feat!:` or `fix!:` creates a major release.
- `docs:`, `test:`, `ci:`, `chore:`, and `style:` are tracked but do not
  normally create a package release by themselves.

## Manual Fallback

The PyPI workflow also supports manual dispatch as a break-glass fallback. The
normal path is release PR merge to `main`, followed by Release Please creating
the GitHub release.

## Recommended Release Posture

The current automation is directionally right: version bumps and changelog
generation should be boring, reviewable, and mostly automated. The important
boundary is that publishing must remain tied to an explicit release artifact,
not to arbitrary commits.

Current steady-state:

1. Feature work lands in `main` via reviewed PRs.
2. CI validates the merge.
3. Release Please opens a release PR that only changes release metadata.
4. A human reviews and merges the release PR.
5. Release Please creates the GitHub tag/release.
6. The PyPI publish workflow runs from the GitHub release/tag.

Do not publish directly from every `main` push. Push-triggered publishing can
publish a package even if GitHub release creation fails. Release-triggered
publishing keeps PyPI, git tags, GitHub Releases, and changelog state aligned.

## Versioning Policy

Use SemVer, with normal `0.x` pre-1.0 semantics:

- `fix:` creates a patch release for compatible bug fixes.
- `feat:` creates a minor release for user-visible CLI capability.
- Breaking CLI changes should be rare; if needed before 1.0, document them
  clearly in the release notes and prefer a minor bump.
- Batch related work into coherent releases instead of publishing every small
  commit independently.

For `axctl`, a good release is one that an operator can understand from the
changelog: what changed, why it matters, and whether any setup or credential
behavior changed.

## Automation Prerequisites

Release Please needs permission to open and update pull requests.

Required setup:

- Add a repository secret named `RELEASE_PLEASE_TOKEN` containing a bot PAT with
  repository Contents: write, Pull requests: write, and Issues: write access.

The workflow intentionally does not fall back to `GITHUB_TOKEN`. Repository and
organization Actions settings can make that token behave differently across
environments. A dedicated bot token gives us an explicit release identity and a
clear failure mode when the secret is missing.

## Follow-Up Hardening

- Keep release-sensitive files covered by CODEOWNERS:
  `.github/workflows/`, `pyproject.toml`, `release-please-config.json`,
  `.release-please-manifest.json`, `CHANGELOG.md`, and this document.
- If Release Please fails after a release PR merge, verify PyPI, create the
  missing GitHub release/tag if needed, and delete any stale generated
  release-please branch before the next release cycle.
