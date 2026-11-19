# SKILL: Azure Kusto Spark Connector — Spark 4.2 Release Process

## Scope

Maintain the Spark 4.2 connector on `release/spark42`. Changes and version bumps
on this branch do not automatically apply to `master` or `release/spark3`.
Those maintenance lines require separately approved and validated backports.

Prepare local changes for review unless the user explicitly authorizes a later
step. Do not push, merge, tag, queue pipelines, upload artifacts, or publish a
release on the user's behalf. Preserve unrelated work and existing releases.

## Repository Context

| Item | Value |
|---|---|
| Repository | `Azure/azure-kusto-spark` |
| Active development branch | `release/spark42` |
| Existing Spark 4.0/4.1 branch | `master` |
| Existing Spark 3 branch | `release/spark3` |
| Maven group | `com.microsoft.azure.kusto` |
| Spark 4.2 artifact | `kusto-spark_4.2_2.13` |
| Spark 4.2 tag | `v4.2_{version}` |
| Version property | `<revision>` in [pom.xml](pom.xml) |
| CI | [.azuredevops/build.yml](.azuredevops/build.yml), Java 17 and 21 |
| Release | [.azuredevops/release.yml](.azuredevops/release.yml), manual only |
| Release staging prefix | `kusto/spark/4.2_{version}` |
| Merge strategy | Reviewed PR, squash merge |

The artifact ID derives from `spark.version.major` and `scala.version.major`,
following the existing Spark 3 and Spark 4 naming convention. Do not reuse the
`kusto-spark_4.0_2.13` coordinate for Spark 4.2-only changes. Keep the connector
revision separate from the Kusto Java SDK version.

## Development

1. Inspect the working tree and identify the official repository remote. Do not
   assume `origin` is official; it may be a fork.
2. Base feature branches on the current reviewed `release/spark42` commit. Use
   `<username>/<type>/<short-description>_4.2`, matching the existing convention
   for runtime-specific feature branches. Create local branches without tracking
   a protected upstream branch.
3. Keep changes scoped, preserve existing product behavior unless intentionally
   changed, and validate the exact candidate. Keep private investigation records
   out of public commits.
4. Use conventional commit/PR titles such as `fix: ...`, `feat: ...`, `ci: ...`
   or `chore: ...`. Open feature PRs against `release/spark42`, not `master`.
5. Require CI and human review before merge. A local build or skipped live test
   is not target-runtime validation. Never weaken a failed required check.
6. Do not automatically cherry-pick or bump another release line. An approved
   backport gets its own branch, version decision, tests and PR.

## Preparing a Release

1. Select an unused connector version for the Spark 4.2 coordinate. Verify
   existing tags, Maven packages and staged artifacts before allocating it.
2. Change only this branch's `<revision>` and release notes. Do not change
   `kusto.sdk.version` merely because the connector revision changes.
3. Move the relevant [Unreleased notes](CHANGELOG.md) into a version-specific
   section while preparing the release. Mark it `Unreleased` until the release
   date is approved. A version in the POM is not evidence of publication.
4. Validate the build, focused regression tests and required target-runtime
   scenarios. Record skipped/blocked tests separately from passes. Check the
   shaded JAR, filtered version resources, flattened POM and artifact identity.
5. Commit only the approved source, configuration, documentation and tests.
   Present the local commit and validation results for review; do not publish.

## Release Handoff

The operator must use an authorized Spark 4.2 release definition whose pipeline
version is `release/spark42`. Register/authorize that entrypoint and reconcile
duplicate publishers before enabling publication. This file does not create
pipeline definitions, service connections or permissions.

After the reviewed commit is available on the official branch, the operator
creates an immutable `v4.2_{version}` tag on that commit. Tag creation does not
automatically publish from this branch.

The manual release parameters are:

| Parameter | Meaning | Default |
|---|---|---|
| `sourceTag` | Exact `v4.2_x.y.z` tag to validate | Empty; required unless queued on the tag |
| `createGitHubRelease` | Explicitly create the GitHub release | `false` |
| `uploadToAzure` | Explicitly upload the staged bundle | `false` |

The pipeline verifies tag ancestry on `release/spark42`, the Spark/Scala artifact
identity, and tag/POM version agreement before Azure-authenticated verification.
It stages the shaded main, sources, Javadoc, original JAR and flattened POM.
Uploads must not overwrite an existing blob. GitHub publication, storage upload,
signing and Maven publication are separate outcomes; verify each required one.

Downstream consumers must explicitly adopt the new coordinate and pin a tested
bundle. Do not update stable runtime consumers as part of a Spark 4.2 release.

## Recovery

- Never delete, move or recreate a published tag to retry a release.
- Never replace bytes under an existing Maven version or bundle identity.
- On partial publication, retain provenance and resume only the missing,
  explicitly approved step; do not blindly rerun every publishing action.
- Do not infer publication or deployment from a successful build or merged PR.
- Keep maintenance/security exceptions separate from routine Spark 4.2 work.