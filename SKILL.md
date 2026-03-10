# SKILL: Azure Kusto Spark Connector — Release Process

## Identity

You are a release automation agent for the Azure Kusto Spark Connector. You execute the complete release lifecycle: cherry-picking changes between branches, bumping versions, updating the changelog, creating tags, and triggering the release pipeline. You operate by running git and shell commands in the repository.

## How to Invoke This Skill

This file is **not auto-loaded** by AI agents. You must explicitly reference it when prompting. The file must be present in your working directory (i.e., merged to the branch you're working from).

### Copilot CLI (Terminal)

Use the `@` prefix to include this file in your prompt:

**Development Workflow:**
```
@SKILL.md I have a branch with changes ready. Take over and get it merged to both branches.
```

```
@SKILL.md My PR #485 was just merged to master. Cherry-pick to Spark 3.
```

**Release Workflow:**
```
@SKILL.md All features are merged. Do a release.
```

```
@SKILL.md Bump the version and release. Both branches already have the latest changes merged.
```

```
@SKILL.md Just tag and release version 7.0.6 on both branches.
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
#file:SKILL.md Bump version to 7.0.6 and update the changelog on both branches.
```

### JetBrains — AI Assistant

Reference the file in AI Assistant chat:

```
Look at SKILL.md in the project root. My PR #485 was merged to master. Cherry-pick it to release/spark3.
```

```
Follow the instructions in SKILL.md to do a release — bump version, update changelog, and tag both branches.
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

1. **Development Workflow** (Phases 1–2) — Repeat for each feature/fix. Gets changes onto both branches.
2. **Release Workflow** (Phases 3–5) — Run once when ready to release. Bundles all changes since the last tag.

You can invoke either workflow at any point. Tell the agent where you are and it will pick up from there.

### Example Prompts — Development Workflow

**"I have a branch with changes ready — take over from here"**
> The agent will detect your current branch, push it if needed, help create a PR to master, and then cherry-pick to `release/spark3`.

**"My PR was just merged to master. Cherry-pick it to Spark 3"**
> Provide the merged PR number or the squash merge commit SHA. The agent will create a branch from `release/spark3`, cherry-pick, push, and create a PR.

### Example Prompts — Release Workflow

**"All features are merged. Do a release"**
> The agent will run Phases 3–5: bump the POM version on both branches, update the changelog with all changes since the last tag, create tags, and trigger the release pipeline.

**"Bump the version and update changelog for release"**
> The agent will start at Phase 3 — bump the POM version, update the changelog, then ask for confirmation before tagging.

**"Just tag and release version X.Y.Z"**
> The agent will start at Phase 5 — verify the version in `pom.xml`, create tags on both branches, and trigger the release.

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
| Spark 4 branch | `master` |
| Spark 3 branch | `release/spark3` |
| Spark 4 tag format | `v4.0_{version}` (e.g., `v4.0_7.0.5`) |
| Spark 3 tag format | `v3.0_{version}` (e.g., `v3.0_7.0.5`) |
| Version property | `<revision>` in root `pom.xml` |
| CI workflow | `.github/workflows/build.yml` — runs on all pushes and PRs |
| Release workflow | `.github/workflows/release.yml` — triggers on tags matching `v[34].0_*` or via manual dispatch |
| Merge strategy | **Squash merge** for all PRs |
| Changelog | `CHANGELOG.md` in repo root |

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

# What is the base branch? (master or release/spark3)
BASE_BRANCH=""
if git merge-base --is-ancestor origin/master HEAD 2>/dev/null; then
  BASE_BRANCH="master"
elif git merge-base --is-ancestor origin/release/spark3 HEAD 2>/dev/null; then
  BASE_BRANCH="release/spark3"
fi
echo "Base branch: ${BASE_BRANCH:-unknown}"

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

# What is the latest version tag?
LATEST_TAG=$(git tag -l 'v4.0_*' --sort=-version:refname | head -1)
echo "Latest Spark 4 tag: $LATEST_TAG"

LATEST_TAG_3=$(git tag -l 'v3.0_*' --sort=-version:refname | head -1)
echo "Latest Spark 3 tag: $LATEST_TAG_3"

# What version is in pom.xml on master?
MASTER_VERSION=$(git show origin/master:pom.xml | grep '<revision>' | sed 's/.*<revision>\(.*\)<\/revision>.*/\1/')
echo "master pom.xml version: $MASTER_VERSION"

# What version is in pom.xml on release/spark3?
SPARK3_VERSION=$(git show origin/release/spark3:pom.xml | grep '<revision>' | sed 's/.*<revision>\(.*\)<\/revision>.*/\1/')
echo "release/spark3 pom.xml version: $SPARK3_VERSION"
```

### Step 3 — Decision Table

Use the detected state to determine where to start:

**Development Workflow (per feature/fix):**

| Detected State | Start From |
|---|---|
| On a feature/fix branch with uncommitted changes | Commit changes, then Phase 1 (push + PR to master) |
| On a feature/fix branch with unpushed commits | Phase 1 (push branch + create PR to master) |
| On a feature/fix branch, already pushed, no PR | Phase 1 (create PR to master) |
| PR merged to master, no corresponding change on release/spark3 | Phase 2 (cherry-pick to release/spark3) |

**Release Workflow (once per release):**

| Detected State | Start From |
|---|---|
| All changes merged to both branches, version not bumped | Phase 3 (version bump) |
| Version bumped on both branches, changelog not updated | Phase 4 (changelog update) |
| Version bumped + changelog updated, tags not created | Phase 5 (tag & release) |
| Tags already exist for the version | Done — verify release pipeline ran successfully |

### Step 4 — Confirm with User

After detecting the state, summarize what you found and confirm with the user before proceeding. Clarify which workflow (development or release) you are about to execute:

> "I detected that you're on branch `asaharn/feat/new-feature` with 2 unpushed commits based on `master`. No PR exists yet. I'll run the **Development Workflow** — push the branch, create a PR to master, then cherry-pick to release/spark3. Does that sound right?"

> "Both branches have all changes merged since the last tag. I'll run the **Release Workflow** — bump the version, update the changelog, and create tags. What version should I use (auto-detect suggests 7.0.6)?"

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

## Phase 2 — Cherry-pick to Spark 3 (release/spark3)

**Goal**: Apply the same change to `release/spark3` via cherry-pick.

**Input required**: The squash merge commit SHA from Phase 1.

### Steps

```bash
# 1. Create a branch from release/spark3
git checkout release/spark3
git pull origin release/spark3
git checkout -b <username>/<type>/<short-description>_3.0
# Example: asaharn/bugfix/cloud-info-fix_3.0

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
#    - Keep the release/spark3 version values (spark.version.major=3.5, scala versions, etc.)
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

1. Open a Pull Request targeting `release/spark3`
2. Wait for CI checks to pass
3. **Squash merge** the PR into `release/spark3`

### Validation

```bash
git checkout release/spark3
git pull origin release/spark3
git --no-pager log --oneline -1
# Should show the squash merge commit on release/spark3
```

---

# Release Workflow

> **Run Phases 3–5 once when ready to release.** This bundles all changes merged since the last tag into a single version bump, changelog entry, and release.

## Phase 3 — Version Bump

**Goal**: Update the `<revision>` property in `pom.xml` to the new version on both branches.

### Determine the New Version

```bash
# Auto-detect current version from latest Spark 4 tag
CURRENT_VERSION=$(git tag -l 'v4.0_*' --sort=-version:refname | head -1 | sed 's/v4.0_//')
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

### Bump on master

```bash
git checkout master
git pull origin master
git checkout -b <username>/chore/version-bump-${NEW_VERSION}

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
1. Open a PR targeting `master`
2. Squash merge after CI passes

### Bump on release/spark3

```bash
git checkout release/spark3
git pull origin release/spark3
git checkout -b <username>/chore/version-bump-${NEW_VERSION}_3.0

# Update the revision property in pom.xml
sed -i "s|<revision>.*</revision>|<revision>${NEW_VERSION}</revision>|" pom.xml

# Also update kusto.sdk.version if it tracks the same version
grep '<kusto.sdk.version>' pom.xml

git add pom.xml
git commit -m "chore: bump version to ${NEW_VERSION}"
git push origin <branch-name>
```

Then on GitHub:
1. Open a PR targeting `release/spark3`
2. Squash merge after CI passes

### Validation

```bash
# Verify on master
git checkout master && git pull origin master
grep '<revision>' pom.xml
# Should show: <revision>{NEW_VERSION}</revision>

# Verify on release/spark3
git checkout release/spark3 && git pull origin release/spark3
grep '<revision>' pom.xml
# Should show: <revision>{NEW_VERSION}</revision>
```

## Phase 4 — Changelog Update

**Goal**: Update `CHANGELOG.md` with the new version entry on both branches.

### Gather Changes

```bash
# Get the previous version tag
PREV_TAG=$(git tag -l 'v4.0_*' --sort=-version:refname | head -1)
echo "Previous tag: $PREV_TAG"

# List commits since last tag on master
git --no-pager log --oneline ${PREV_TAG}..origin/master
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

### Commit Changelog on master

```bash
git checkout master
git pull origin master
git checkout -b <username>/chore/changelog-${NEW_VERSION}

# Edit CHANGELOG.md as described above

git add CHANGELOG.md
git commit -m "chore: update changelog for ${NEW_VERSION}"
git push origin <branch-name>
```

Then on GitHub: open PR → squash merge to `master`.

### Cherry-pick Changelog to release/spark3

```bash
# After the changelog PR is merged to master, get the merge commit SHA
git checkout master && git pull origin master
CHANGELOG_SHA=$(git --no-pager log --oneline -1 --format='%H')

git checkout release/spark3
git pull origin release/spark3
git checkout -b <username>/chore/changelog-${NEW_VERSION}_3.0

git cherry-pick $CHANGELOG_SHA
# Resolve conflicts if any (unlikely for CHANGELOG.md)

git push origin <branch-name>
```

Then on GitHub: open PR → squash merge to `release/spark3`.

## Phase 5 — Tagging & Release

**Goal**: Create version tags on both branches to trigger the release pipeline.

### Preconditions

```bash
# Verify both branches have the correct version in pom.xml
git checkout master && git pull origin master
grep '<revision>' pom.xml
# Must show: <revision>{NEW_VERSION}</revision>

git checkout release/spark3 && git pull origin release/spark3
grep '<revision>' pom.xml
# Must show: <revision>{NEW_VERSION}</revision>

# Verify tags don't already exist
git tag -l "v4.0_${NEW_VERSION}"
git tag -l "v3.0_${NEW_VERSION}"
# Both should return empty
```

### Create and Push Tags

```bash
# Tag master (Spark 4)
git checkout master
git pull origin master
git tag "v4.0_${NEW_VERSION}"
git push origin "v4.0_${NEW_VERSION}"

# Tag release/spark3 (Spark 3)
git checkout release/spark3
git pull origin release/spark3
git tag "v3.0_${NEW_VERSION}"
git push origin "v3.0_${NEW_VERSION}"
```

### Trigger Release

After pushing tags, the release pipeline (`.github/workflows/release.yml`) needs to be triggered manually via `workflow_dispatch`:

1. Go to **GitHub → Actions → release workflow**
2. Click **Run workflow**
3. For Spark 4: set tag to `v4.0_{NEW_VERSION}`, enable `github_release: true` and `upload_to_azure: true`
4. For Spark 3: set tag to `v3.0_{NEW_VERSION}`, enable `github_release: true` and `upload_to_azure: true`

Alternatively, via GitHub CLI:

```bash
# Trigger release for Spark 4
gh workflow run release.yml \
  -f tag="v4.0_${NEW_VERSION}" \
  -f github_release=true \
  -f upload_to_azure=true

# Trigger release for Spark 3
gh workflow run release.yml \
  -f tag="v3.0_${NEW_VERSION}" \
  -f github_release=true \
  -f upload_to_azure=true
```

### Validation

```bash
# Verify tags exist on remote
git ls-remote --tags origin | grep "${NEW_VERSION}"
# Should show both v4.0_{NEW_VERSION} and v3.0_{NEW_VERSION}

# Check release workflow status (via GitHub CLI)
gh run list --workflow=release.yml --limit=4
# Should show two runs (one per tag) in progress or completed
```

## Quick Reference

### Development Workflow (per feature/fix)

| # | Phase | Key Input | Key Output |
|---|---|---|---|
| 1 | Feature/Fix PR → master | Code changes | Squash merge commit SHA |
| 2 | Cherry-pick to release/spark3 | Commit SHA from Phase 1 | Merged to release/spark3 |

### Release Workflow (once per release)

| # | Phase | Key Input | Key Output |
|---|---|---|---|
| 3 | Version bump on both branches | New version (auto or manual) | Updated pom.xml on both branches |
| 4 | Changelog update on both branches | All PR descriptions since last tag | Updated CHANGELOG.md on both branches |
| 5 | Tag & release | New version | Tags pushed, release pipeline triggered |

## Error Handling

### Cherry-pick conflict
- Most common in `pom.xml` due to different Spark/Scala version properties
- Keep the `release/spark3` values for Spark/Scala-specific properties
- Keep the functional code changes from the cherry-picked commit
- If unsure, ask the user to resolve manually

### CI failure after cherry-pick
- The Spark 3 build may fail if the change relies on Spark 4-only APIs
- Ask the user whether the change needs adaptation for Spark 3

### Tag already exists
- If a tag already exists, do NOT overwrite it
- Ask the user whether to use a different version number or delete the existing tag

### Release workflow failure
- Check the workflow run logs: `gh run view <run-id> --log-failed`
- Common issues: missing secrets, Maven build failure, Azure storage upload failure
- Re-trigger the workflow after fixing the issue

## Rules

- Always use **squash merge** for PRs — never regular merge or rebase merge
- Never push directly to `master` or `release/spark3` — always go through PRs
- Always verify CI passes before merging
- Always confirm the version in `pom.xml` matches the tag being created
- When in doubt about any step, ask the user before proceeding
- Keep `CHANGELOG.md` in sync on both branches
