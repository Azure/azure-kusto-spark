# SKILL: Azure Kusto Spark Connector — Release Process

## Identity

You are a release automation agent for the Azure Kusto Spark Connector. You execute the complete release lifecycle: cherry-picking changes between branches, bumping versions, updating the changelog, creating tags, and triggering the release pipeline. You operate by running git and shell commands in the repository.

## How to Invoke This Skill

This file is **not auto-loaded** by AI agents. You must explicitly reference it when prompting. The file must be present in your working directory (i.e., merged to the branch you're working from).

### Copilot CLI (Terminal)

Use the `@` prefix to include this file in your prompt:

**Development Workflow:**
```
@SKILL.md I have a branch with changes ready. Take over and get it merged to master and the maintenance lines that need it.
```

```
@SKILL.md My PR #485 was just merged to master. Cherry-pick to Spark 3.
```

**Release Workflow:**
```
@SKILL.md All features are merged. Do a release.
```

```
@SKILL.md Bump the version and release the Spark 4.2 line. master already has the latest changes merged.
```

```
@SKILL.md Just tag and release version 7.1.8 on the Spark 4.2 line (master).
```

### VS Code — GitHub Copilot Chat

Use `#file:SKILL.md` to reference this file, or `@workspace` to let Copilot find it:

**Development Workflow:**
```
#file:SKILL.md My PR was just merged to master (commit abc1234). Cherry-pick to release/spark3.
```

```
@workspace Follow SKILL.md. I have changes on branch asaharn/feat/my-feature ready to push.
```

**Release Workflow:**
```
#file:SKILL.md Bump the Spark 4.0 line (release/spark40) to 7.1.7 and update its changelog.
```

### JetBrains — AI Assistant

Reference the file in AI Assistant chat:

```
Look at SKILL.md in the project root. My PR #485 was merged to master. Cherry-pick it to release/spark3.
```

```
Follow the instructions in SKILL.md to do a release — bump version, update changelog, and tag the selected release line(s).
```

### GitHub.com — Copilot in PR / Issues

When using Copilot on GitHub.com (e.g., in a PR comment or issue), reference the file path:

```
Follow the development workflow in SKILL.md at the repo root. This PR is now merged — describe the next steps to cherry-pick to release/spark3.
```

> **Note**: GitHub.com Copilot cannot execute git commands directly. It will provide the commands for you to run, or guide you through the GitHub UI steps.

### Prompt Tips

- **Be specific about where you are**: "My PR is merged", "I have a branch ready", "Both branches are updated"
- **Provide commit SHAs or PR numbers** when starting from Phase 2 (cherry-pick)
- **Specify a version** if you don't want auto-detection (e.g., "release version 7.0.6")
- **The agent will ask** for missing info — you don't need to provide everything upfront

## How to Use This Skill

This skill has **two independent workflows** that can be invoked separately:

1. **Development Workflow** (Phases 1–2) — Repeat for each feature/fix. Gets changes onto `master` and then onto each maintenance line that needs them.
2. **Release Workflow** (Phases 3–5) — Run once per release line when ready to release. Bundles all changes on that line since its last tag.

There are three independent release lines (see [Release Lines](#release-lines)). Always confirm with the user which line(s) a change or release targets.

You can invoke either workflow at any point. Tell the agent where you are and it will pick up from there.

### Example Prompts — Development Workflow

**"I have a branch with changes ready — take over from here"**
> The agent will detect your current branch, push it if needed, help create a PR to master, and then cherry-pick to each maintenance line the user selects (`release/spark40`, `release/spark3`).

**"My PR was just merged to master. Cherry-pick it to Spark 4.0 and Spark 3"**
> Provide the merged PR number or the squash merge commit SHA. For each target line, the agent will create a branch from that line's branch, cherry-pick, push, and create a PR.

### Example Prompts — Release Workflow

**"All features are merged. Do a release"**
> The agent will ask which release line(s) to release, then run Phases 3–5 for each: bump the POM version, update the changelog with all changes since that line's last tag, create the tag, and trigger that line's release pipeline.

**"Bump the version and update changelog for release"**
> The agent will start at Phase 3 — bump the POM version, update the changelog, then ask for confirmation before tagging.

**"Just tag and release version X.Y.Z"**
> The agent will start at Phase 5 — for each selected line, verify the version in `pom.xml`, create that line's tag, and trigger the release.

### What the Agent May Ask You

If information is missing, the agent should ask for:

| Missing Info | When Needed |
|---|---|
| Squash merge commit SHA | Development Workflow — Phase 2 (cherry-pick) |
| Version number (if not auto-detecting) | Release Workflow — Phase 3+ |
| Change descriptions for changelog | Release Workflow — Phase 4, if not derivable from PRs |
| Confirmation before pushing tags | Release Workflow — always before Phase 5 |

## Repository Context

| Item | Value |
|---|---|
| Repository | `Azure/azure-kusto-spark` |
| Spark 4.2 branch | `master` (artifact `kusto-spark_4.2_2.13`) |
| Spark 4.0/4.1 branch | `release/spark40` (artifact `kusto-spark_4.0_2.13`) |
| Spark 3 branch | `release/spark3` (artifact `kusto-spark_3.0_2.12`) |
| Spark 4.2 tag format | `v4.2_{version}` (e.g., `v4.2_7.1.7`) |
| Spark 4.0/4.1 tag format | `v4.0_{version}` (e.g., `v4.0_7.1.6`) |
| Spark 3 tag format | `v3.0_{version}` (e.g., `v3.0_7.0.5`) |
| Version property | `<revision>` in root `pom.xml` |
| CI pipeline | `.azuredevops/build.yml` — Azure Pipelines build, runs on pushes and PRs |
| Release pipeline (Spark 4) | `.azuredevops/release.yml` — Azure Pipelines release for `v4.2_` tags on `master` and `v4.0_` tags on `release/spark40`. Its trigger lists `v3.0_*.*.*`, `v4.0_*.*.*` and `v4.2_*.*.*` tags. A tag push runs the YAML from the tagged commit. A manual run uses the YAML from the branch you pick and only checks out and builds the tag given in `sourceTag`. Checks that the tag matches the POM |
| Release workflow (Spark 3) | `.github/workflows/release.yml` on `release/spark3` — GitHub Actions, triggers on tags matching `v[34].0_[0-9]*.[0-9]*.[0-9]*`. This is the normal release path for `v3.0_` tags (`release/spark3` has no `.azuredevops/release.yml`). Does **not** check that the tag matches the POM |
| Merge strategy | **Squash merge** for all PRs |
| Changelog | `CHANGELOG.md` in repo root (each branch keeps its own) |

## Release Lines

| Line | Branch | Tag prefix | Maven artifact | Cherry-pick branch suffix | Release pipeline |
|---|---|---|---|---|---|
| Spark 4.2 | `master` | `v4.2_` | `kusto-spark_4.2_2.13` | — | Azure Pipelines `.azuredevops/release.yml` |
| Spark 4.0 / 4.1 | `release/spark40` | `v4.0_` | `kusto-spark_4.0_2.13` | `_4.0` | Azure Pipelines `.azuredevops/release.yml` (from the tagged commit) |
| Spark 3 | `release/spark3` | `v3.0_` | `kusto-spark_3.0_2.12` | `_3.0` | GitHub Actions `.github/workflows/release.yml` on `release/spark3` |

Rules:

- New work lands on `master` first, then is cherry-picked to each maintenance line that needs it.
- Lines are released **independently** and may have different versions and changelogs. Never create a tag on a branch that belongs to another line. The tag must equal `v{spark.version.major}_{revision}` of the tagged commit.
  - Spark 4 lines: the Azure release pipeline (`.azuredevops/release.yml`) enforces this and fails on a mismatch.
  - Spark 3: the GitHub Actions workflow does **not** check it. It accepts any tag that matches its trigger pattern and takes the published version from the POM, so a wrong tag can still publish. Always run the tag/POM check in [Preconditions](#preconditions) before tagging.
- Never release a version that is already published for the same artifact. Different lines may share a version number (e.g. `kusto-spark_4.2_2.13:7.1.7` and `kusto-spark_4.0_2.13:7.1.7`) because the artifacts differ.
- Set the line once and use these variables in every Release Workflow command:

```bash
LINE=4.2   # one of: 4.2 | 4.0 | 3.0
case "$LINE" in
  4.2) LINE_BRANCH=master;          TAG_PREFIX=v4.2_; SUFFIX="" ;;
  4.0) LINE_BRANCH=release/spark40; TAG_PREFIX=v4.0_; SUFFIX=_4.0 ;;
  3.0) LINE_BRANCH=release/spark3;  TAG_PREFIX=v3.0_; SUFFIX=_3.0 ;;
esac
POM=$(git show "origin/${LINE_BRANCH}:pom.xml")
ARTIFACT="kusto-spark_$(sed -n 's|.*<spark.version.major>\(.*\)</spark.version.major>.*|\1|p' <<<"$POM")_$(sed -n 's|.*<scala.version.major>\(.*\)</scala.version.major>.*|\1|p' <<<"$POM")"
echo "Line ${LINE}: branch=${LINE_BRANCH} tag=${TAG_PREFIX}<version> artifact=${ARTIFACT}"
```

## Prerequisites

Before starting any phase, verify:

```bash
# 1. Working tree is clean
git status --porcelain
# Expected: empty output

# 2. Remote is up to date
git fetch origin

# 3. You are on the correct branch
git branch --show-current
```

## State Detection

When the user asks you to take over, run these commands to determine the current state and decide which phase to start from.

### Step 1 — Detect Current Branch and Changes

```bash
# What branch are we on?
CURRENT_BRANCH=$(git branch --show-current)
echo "Current branch: $CURRENT_BRANCH"

# Are there uncommitted changes?
DIRTY=$(git status --porcelain)
echo "Uncommitted changes: ${DIRTY:-none}"

# What is the base branch? Pick the line branch whose merge-base is closest to HEAD.
BASE_BRANCH=""; BEST=""; TIE=""
for b in master release/spark40 release/spark3; do
  mb=$(git merge-base "origin/$b" HEAD 2>/dev/null) || continue
  n=$(git rev-list --count "${mb}..HEAD")
  if [[ -z "$BEST" || "$n" -lt "$BEST" ]]; then BEST=$n; BASE_BRANCH=$b; TIE=""
  elif [[ "$n" -eq "$BEST" ]]; then TIE="$TIE $b"; fi
done
echo "Base branch: ${BASE_BRANCH:-unknown}${TIE:+ (ambiguous with:$TIE — ask the user)}"

# Are there unpushed commits?
UNPUSHED=$(git --no-pager log origin/${CURRENT_BRANCH}..HEAD --oneline 2>/dev/null || echo "branch not on remote")
echo "Unpushed commits: ${UNPUSHED:-none}"

# Does a remote branch exist?
REMOTE_EXISTS=$(git ls-remote --heads origin "$CURRENT_BRANCH" 2>/dev/null)
echo "Remote branch exists: ${REMOTE_EXISTS:+yes}"
```

### Step 2 — Check PR and Tag State

```bash
# Check for open PRs from this branch (requires gh CLI)
gh pr list --head "$CURRENT_BRANCH" --state all --json number,state,title,mergeCommit 2>/dev/null || echo "gh CLI not available"

# Latest tag and pom version for every release line
git fetch --tags origin
for pair in master:v4.2_ release/spark40:v4.0_ release/spark3:v3.0_; do
  b=${pair%%:*}; t=${pair#*:}
  tag=$(git tag -l "${t}*" --sort=-version:refname | head -1)
  rev=$(git show "origin/$b:pom.xml" | sed -n 's|.*<revision>\(.*\)</revision>.*|\1|p')
  echo "$b: latest tag=${tag:-none}, pom revision=$rev"
done
```

### Step 3 — Decision Table

Use the detected state to determine where to start:

**Development Workflow (per feature/fix):**

| Detected State | Start From |
|---|---|
| On a feature/fix branch with uncommitted changes | Commit changes, then Phase 1 (push + PR to master) |
| On a feature/fix branch with unpushed commits | Phase 1 (push branch + create PR to master) |
| On a feature/fix branch, already pushed, no PR | Phase 1 (create PR to master) |
| PR merged to master, no corresponding change on a maintenance line that needs it | Phase 2 (cherry-pick to `release/spark40` and/or `release/spark3`) |

**Release Workflow (once per release):**

| Detected State | Start From |
|---|---|
| All changes merged to the line's branch, version not bumped | Phase 3 (version bump) |
| Version bumped on the line's branch, changelog not updated | Phase 4 (changelog update) |
| Version bumped + changelog updated, tags not created | Phase 5 (tag & release) |
| Tag `${TAG_PREFIX}{version}` already exists | Done — verify release pipeline ran successfully |

### Step 4 — Confirm with User

After detecting the state, summarize what you found and confirm with the user before proceeding. Clarify which workflow (development or release) you are about to execute:

> "I detected that you're on branch `asaharn/feat/new-feature` with 2 unpushed commits based on `master`. No PR exists yet. I'll run the **Development Workflow** — push the branch, create a PR to master, then cherry-pick to the maintenance lines you choose (`release/spark40`, `release/spark3`). Does that sound right?"

> "`master` (Spark 4.2) has changes since `v4.2_7.1.7`. I'll run the **Release Workflow** for the Spark 4.2 line — bump the version, update the changelog, and create the `v4.2_` tag. Should `release/spark40` or `release/spark3` be released too, and what version should I use (auto-detect suggests 7.1.8)?"

---

# Development Workflow

> **Repeat Phases 1–2 for each feature or fix.** Multiple features can be developed and cherry-picked independently before doing a release.

## Phase 1 — Feature / Fix Development (on master)

**Goal**: Implement a change on a branch forked from `master`, get it merged via PR.

### Commit & PR Title Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for PR titles and commit messages:

```
<type>: <short description>
```

| Type | Use for |
|---|---|
| `feat` | New features or capabilities |
| `fix` | Bug fixes |
| `chore` | Version bumps, dependency updates, maintenance |
| `docs` | Documentation changes |
| `refactor` | Code restructuring without behavior change |
| `ci` | CI/CD pipeline changes |
| `test` | Adding or updating tests |

Examples: `feat: add storage protocol option`, `fix: resolve CloudInfo cache issue`, `chore: bump version to 7.0.6`

### Steps

```bash
# 1. Create a feature/fix branch from latest master
git checkout master
git pull origin master
git checkout -b <username>/<type>/<short-description>
# Example: asaharn/feat/new-read-mode  or  asaharn/fix/cloud-info-fix

# 2. Implement the change
# ... make code changes ...

# 3. Commit and push (use conventional commit message)
git add -A
git commit -m "<type>: <short description>"
git push origin <branch-name>
```

### Then on GitHub

1. Open a Pull Request targeting `master` — use conventional commit format for the PR title (e.g., `feat: add storage protocol option`)
2. Fill in the PR template (Breaking Changes / Features / Fixes sections)
3. Wait for CI checks (`build.yml`) to pass
4. Get the PR reviewed and approved
5. **Squash merge** the PR into `master`
6. Note the **squash merge commit SHA** from the merged PR — you will need it for Phase 2

### Validation

```bash
# Confirm the merge commit is on master
git checkout master
git pull origin master
git --no-pager log --oneline -1
# Should show the squash merge commit
```

## Phase 2 — Cherry-pick to maintenance lines (release/spark40, release/spark3)

**Goal**: Apply the same change to each maintenance line that needs it via cherry-pick. Repeat these steps per target line, with `TARGET` = `release/spark40` (suffix `_4.0`) or `release/spark3` (suffix `_3.0`).

**Input required**: The squash merge commit SHA from Phase 1, and the list of target lines (ask the user).

### Steps

```bash
TARGET=release/spark40   # or release/spark3
SUFFIX=_4.0              # or _3.0

# 1. Create a branch from the target line
git checkout "$TARGET"
git pull origin "$TARGET"
git checkout -b <username>/<type>/<short-description>${SUFFIX}
# Example: asaharn/bugfix/cloud-info-fix_4.0

# 2. Cherry-pick the squash merge commit from master
git cherry-pick <squash-merge-commit-sha>
```

### Handling Cherry-pick Conflicts

If `git cherry-pick` reports conflicts:

```bash
# 1. List conflicted files
git diff --name-only --diff-filter=U

# 2. Open each conflicted file and resolve conflicts manually
#    - Conflicts are typically in pom.xml due to version differences
#    - Keep the target line's values (revision, spark/scala/hadoop/netty versions, etc.)
#    - Keep the functional code changes from the cherry-picked commit

# 3. After resolving all conflicts
git add -A
git cherry-pick --continue
```

### Push and Merge

```bash
# 3. Push the branch
git push origin <branch-name>
```

Then on GitHub:

1. Open a Pull Request targeting `$TARGET`
2. Wait for CI checks to pass
3. **Squash merge** the PR into `$TARGET`

### Validation

```bash
git checkout "$TARGET"
git pull origin "$TARGET"
git --no-pager log --oneline -1
# Should show the squash merge commit on $TARGET
```

---

# Release Workflow

> **Run Phases 3–5 once per release line.** Set `LINE`, `LINE_BRANCH`, `TAG_PREFIX` and `SUFFIX` as in [Release Lines](#release-lines) first. This bundles all changes merged to that line since its last tag into a single version bump, changelog entry, and release.

## Phase 3 — Version Bump

**Goal**: Update the `<revision>` property in `pom.xml` to the new version on the selected line's branch.

### Determine the New Version

```bash
# Auto-detect the current version: highest of the line's latest tag and its pom revision
git fetch --tags origin
TAG_VERSION=$(git tag -l "${TAG_PREFIX}*" --sort=-version:refname | head -1 | sed "s/^${TAG_PREFIX}//")
POM_VERSION=$(git show "origin/${LINE_BRANCH}:pom.xml" | sed -n 's|.*<revision>\(.*\)</revision>.*|\1|p')
CURRENT_VERSION=$(printf '%s\n' ${TAG_VERSION} ${POM_VERSION} | sort -V | tail -1)
echo "Current version: $CURRENT_VERSION"

# Compute next patch version
MAJOR=$(echo $CURRENT_VERSION | cut -d. -f1)
MINOR=$(echo $CURRENT_VERSION | cut -d. -f2)
PATCH=$(echo $CURRENT_VERSION | cut -d. -f3)
NEXT_PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEXT_PATCH}"
echo "Next version: $NEW_VERSION"
```

**Manual override**: If the user specifies a version, use that instead of auto-detection.

Before continuing, make sure the version is not already published for this line's artifact (HTTP 404 expected):

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  "https://repo1.maven.org/maven2/com/microsoft/azure/kusto/${ARTIFACT}/${NEW_VERSION}/${ARTIFACT}-${NEW_VERSION}.pom"
git ls-remote --tags origin "refs/tags/${TAG_PREFIX}${NEW_VERSION}"   # expected: empty
```

### Bump on the line's branch

```bash
git checkout "$LINE_BRANCH"
git pull origin "$LINE_BRANCH"
git checkout -b <username>/chore/version-bump-${NEW_VERSION}${SUFFIX}

# Update the revision property in pom.xml
sed -i "s|<revision>.*</revision>|<revision>${NEW_VERSION}</revision>|" pom.xml

# Also update kusto.sdk.version if it tracks the same version
# Check first:
grep '<kusto.sdk.version>' pom.xml
# Only update if it matches the old version

git add pom.xml
git commit -m "chore: bump version to ${NEW_VERSION}"
git push origin <branch-name>
```

Then on GitHub:
1. Open a PR targeting `$LINE_BRANCH`
2. Squash merge after CI passes

### Validation

```bash
git checkout "$LINE_BRANCH" && git pull origin "$LINE_BRANCH"
grep '<revision>' pom.xml
# Should show: <revision>{NEW_VERSION}</revision>
```

## Phase 4 — Changelog Update

**Goal**: Update `CHANGELOG.md` with the new version entry on the selected line's branch. Each line keeps its own changelog; the release pipeline publishes the `## [{NEW_VERSION}]` section of the tagged commit and fails if it is missing.

### Gather Changes

```bash
# Get the line's previous version tag
PREV_TAG=$(git tag -l "${TAG_PREFIX}*" --sort=-version:refname | head -1)
echo "Previous tag: ${PREV_TAG:-none}"
# First release on a line (no tag with this prefix yet): ask the user for the baseline,
# e.g. the predecessor line's last tag (v4.0_7.1.6 for the first v4.2_ release) or a commit SHA.
if [[ -z "$PREV_TAG" ]]; then
  read -r -p "No ${TAG_PREFIX} tag yet. Baseline tag/commit: " PREV_TAG
fi

# List commits since that tag on the line's branch
git --no-pager log --oneline ${PREV_TAG}..origin/${LINE_BRANCH}
```

### Update CHANGELOG.md

Edit `CHANGELOG.md`:

1. Move items from the `[Unreleased]` section into a new version section
2. Create a new `[Unreleased]` section with empty subsections
3. Add a new version heading: `## [{NEW_VERSION}] - YYYY-MM-DD` (use today's date)
4. Categorize changes under: `### Added`, `### Changed`, `### Fixed`, `### Removed`
5. Source descriptions from the PR titles and PR template sections

The changelog entry should look like:

```markdown
## [Unreleased]

### Changed
- None

### Added
- None

### Fixed
- None

## [{NEW_VERSION}] - YYYY-MM-DD

### Changed
- <items or "None">

### Added
- <items or "None">

### Fixed
- <items or "None">
```

### Commit Changelog on the line's branch

```bash
git checkout "$LINE_BRANCH"
git pull origin "$LINE_BRANCH"
git checkout -b <username>/chore/changelog-${NEW_VERSION}${SUFFIX}

# Edit CHANGELOG.md as described above

git add CHANGELOG.md
git commit -m "chore: update changelog for ${NEW_VERSION}"
git push origin <branch-name>
```

Then on GitHub: open PR → squash merge to `$LINE_BRANCH`.

## Phase 5 — Tagging & Release

**Goal**: Create the selected line's version tag on that line's branch to trigger its release pipeline. Repeat per line being released.

### Preconditions

Run this block from the repository root. It stops at the first failed check and prints `PRECONDITIONS OK` only when every check passes. Every check reads the committed files of the merged branch tip, not the working tree.

```bash
(
  set -euo pipefail
  git fetch --tags origin
  git checkout "$LINE_BRANCH"
  git pull --ff-only origin "$LINE_BRANCH"

  # Release only what was merged: local HEAD must equal the remote branch tip
  RELEASE_COMMIT=$(git rev-parse HEAD)
  if [ "$RELEASE_COMMIT" != "$(git rev-parse "origin/$LINE_BRANCH")" ]; then
    echo "STOP: local $LINE_BRANCH is not equal to origin/$LINE_BRANCH" >&2
    exit 1
  fi

  # The tag must equal v{spark.version.major}_{revision} of this commit.
  # Required for Spark 3: its GitHub Actions release workflow does not check this.
  POM=$(git show "$RELEASE_COMMIT:pom.xml")
  SPARK_MAJOR=$(sed -n 's|.*<spark.version.major>\(.*\)</spark.version.major>.*|\1|p' <<<"$POM")
  POM_REVISION=$(sed -n 's|.*<revision>\(.*\)</revision>.*|\1|p' <<<"$POM")
  if [ "v${SPARK_MAJOR}_${POM_REVISION}" != "${TAG_PREFIX}${NEW_VERSION}" ]; then
    echo "STOP: tag ${TAG_PREFIX}${NEW_VERSION} does not match POM v${SPARK_MAJOR}_${POM_REVISION}" >&2
    exit 1
  fi

  # The tag must not exist yet (locally or on the remote)
  # Standalone assignments, so set -e stops on a failed lookup instead of treating it as "absent"
  LOCAL_TAG=$(git tag -l "${TAG_PREFIX}${NEW_VERSION}")
  REMOTE_TAG=$(git ls-remote --tags origin "refs/tags/${TAG_PREFIX}${NEW_VERSION}")
  if [ -n "$LOCAL_TAG" ] || [ -n "$REMOTE_TAG" ]; then
    echo "STOP: tag ${TAG_PREFIX}${NEW_VERSION} already exists" >&2
    exit 1
  fi

  # CHANGELOG.md must contain the dated section the release pipeline publishes
  CHANGELOG=$(git show "$RELEASE_COMMIT:CHANGELOG.md")
  grep -q "^## \[${NEW_VERSION}\] - " <<<"$CHANGELOG" || { echo "STOP: no dated CHANGELOG section for ${NEW_VERSION}" >&2; exit 1; }

  echo "PRECONDITIONS OK"
)
```

### Create and Push the Tag

Run on `$LINE_BRANCH` only, and only after `PRECONDITIONS OK`. This block repeats the branch-tip and tag/POM checks, then tags that exact commit. On any mismatch it exits with an error and creates no tag:

```bash
(
  set -euo pipefail
  RELEASE_COMMIT=$(git rev-parse HEAD)
  REMOTE_TIP=$(git ls-remote origin "refs/heads/$LINE_BRANCH")
  if [ "$RELEASE_COMMIT" != "${REMOTE_TIP%%[[:space:]]*}" ]; then
    echo "STOP: HEAD is not the current tip of origin/$LINE_BRANCH" >&2
    exit 1
  fi
  POM=$(git show "$RELEASE_COMMIT:pom.xml")
  EXPECTED_TAG="v$(sed -n 's|.*<spark.version.major>\(.*\)</spark.version.major>.*|\1|p' <<<"$POM")_$(sed -n 's|.*<revision>\(.*\)</revision>.*|\1|p' <<<"$POM")"
  if [ "$EXPECTED_TAG" != "${TAG_PREFIX}${NEW_VERSION}" ]; then
    echo "STOP: tag ${TAG_PREFIX}${NEW_VERSION} does not match POM ${EXPECTED_TAG}" >&2
    exit 1
  fi
  git tag "${TAG_PREFIX}${NEW_VERSION}" "$RELEASE_COMMIT"
  git push origin "refs/tags/${TAG_PREFIX}${NEW_VERSION}"
)
```

### Trigger Release

- **Spark 4.2 (`v4.2_`) and Spark 4.0/4.1 (`v4.0_`)**: the Azure release pipeline (`.azuredevops/release.yml` at the tagged commit) runs automatically when the tag is pushed. To run it manually (for example to publish with the optional steps enabled):
  1. Go to **Azure DevOps → Pipelines → the azure-kusto-spark release pipeline**
  2. Click **Run pipeline**
  3. Set `sourceTag` to `${TAG_PREFIX}{NEW_VERSION}` and enable `createGitHubRelease` and `uploadToAzure`

  Or via Azure CLI (requires the `azure-devops` extension):

  ```bash
  az pipelines run --name "<release-pipeline-name>" \
    --branch "refs/tags/${TAG_PREFIX}${NEW_VERSION}" \
    --parameters sourceTag="${TAG_PREFIX}${NEW_VERSION}" createGitHubRelease=true uploadToAzure=true
  ```

- **Spark 3 (`v3.0_`)**: `release/spark3` has no Azure Pipelines release definition. Pushing the tag triggers the GitHub Actions workflow `.github/workflows/release.yml` from the tagged commit. It cannot be started manually from the Actions tab (the workflow is not on the default branch); to re-run, use **Re-run jobs** on the tag's existing run.

### Validation

```bash
# Verify the tag exists on the remote
git ls-remote --tags origin "refs/tags/${TAG_PREFIX}${NEW_VERSION}"

# Spark 4 lines: check the Azure release pipeline run
PIPELINE_ID="$(az pipelines show --name "<release-pipeline-name>" --query id --output tsv)"
az pipelines runs list --pipeline-ids "${PIPELINE_ID}" --top 3

# Spark 3: check the GitHub Actions run
gh run list --workflow release.yml --limit 3
```

## Quick Reference

### Development Workflow (per feature/fix)

| # | Phase | Key Input | Key Output |
|---|---|---|---|
| 1 | Feature/Fix PR → master | Code changes | Squash merge commit SHA |
| 2 | Cherry-pick to release/spark40 and/or release/spark3 (same steps as Phase 2) | Commit SHA from Phase 1 | Merged to the maintenance branch |

### Release Workflow (once per release line)

| # | Phase | Key Input | Key Output |
|---|---|---|---|
| 3 | Version bump on the line's branch | Line + new version (auto or manual) | Updated pom.xml on `$LINE_BRANCH` |
| 4 | Changelog update on the line's branch | All PR descriptions since the line's last tag | Updated CHANGELOG.md on `$LINE_BRANCH` |
| 5 | Tag & release | Line + new version | `${TAG_PREFIX}{version}` pushed, line's release pipeline triggered |

## Error Handling

### Cherry-pick conflict
- Most common in `pom.xml` due to different Spark/Scala version properties
- Keep the maintenance branch's values (`release/spark40` or `release/spark3`) for Spark/Scala/Netty-specific properties
- Keep the functional code changes from the cherry-picked commit
- If unsure, ask the user to resolve manually

### CI failure after cherry-pick
- A maintenance-line build may fail if the change relies on APIs only available in newer Spark/Hadoop versions (e.g. Spark 4.2 / Hadoop 3.5 on master)
- Ask the user whether the change needs adaptation for that line

### Tag already exists
- If a tag already exists, do NOT overwrite it
- Ask the user whether to use a different version number or delete the existing tag

### Release workflow failure
- Spark 4 lines: check the pipeline run logs in Azure DevOps (`az pipelines runs show --id <run-id>`); Spark 3: check the GitHub Actions run (`gh run view <run-id> --log-failed`)
- Common issues: missing variable group values or service connection approval, Maven build failure, Azure storage upload failure
- Re-run the pipeline after fixing the issue

## Rules

- Always use **squash merge** for PRs — never regular merge or rebase merge
- Never push directly to `master`, `release/spark40` or `release/spark3` — always go through PRs
- Always verify CI passes before merging
- Always confirm the version in `pom.xml` matches the tag being created
- When in doubt about any step, ask the user before proceeding
- Keep each line's `CHANGELOG.md` accurate for that line; cherry-pick shared entries when a change lands on several lines
