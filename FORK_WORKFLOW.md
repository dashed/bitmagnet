# Custom Bitmagnet Fork Workflow

This document describes the branch structure and workflow for maintaining a custom bitmagnet fork with stacked feature branches.

## Overview

This fork uses a **stacked branch strategy** where:

- `main` tracks upstream bitmagnet releases
- Features are stacked as a chain, each building on the previous
- `alberto/my-bitmagnet` is the integration branch combining all features
- Jujutsu (jj) is used for version control alongside Git

This approach allows:

- Easy updates when upstream releases new versions
- Automatic rebasing of entire feature chain
- Clean, linear history
- Simple conflict resolution

## Branch Structure

```
main (upstream)
│
└── dev
    └── alberto/search-filters
        │   └── Torrent size filter functionality
        │
        └── alberto/search-filters-published-at
            │   └── Published-at filter
            │
            └── alberto/my-bitmagnet  ← Integration branch
                └── Rebuilt webui + combines all features
```

### Branch Descriptions

| Branch                                | Purpose                   | Base                        |
| ------------------------------------- | ------------------------- | --------------------------- |
| `main`                                | Tracks upstream bitmagnet | upstream (author remote)    |
| `alberto/search-filters`              | Torrent size filter       | main                        |
| `alberto/search-filters-published-at` | Published-at filter       | search-filters              |
| `alberto/my-bitmagnet`                | Integration branch        | search-filters-published-at |

### Visual DAG

```
◆  main (upstream)
│
○  dev
│  - Development setup
│
○  alberto/search-filters
│  - feat: Torrent size filter
│
○  alberto/search-filters-published-at
│  - feat: Filter on published at
│
○  alberto/my-bitmagnet  ← Use this branch!
   - integration: rebuilt webui with all features
```

## Git Remotes

| Remote   | URL                                       | Purpose                       |
| -------- | ----------------------------------------- | ----------------------------- |
| `author` | git@github.com:bitmagnet-io/bitmagnet.git | Upstream (fetch new releases) |
| `origin` | git@github.com:dashed/bitmagnet.git       | Fork (push changes)           |

## Jujutsu (jj) Setup

This repo uses jj in colocated mode, meaning both `jj` and `git` commands work.

### Why jj?

- **Automatic rebasing**: When you update a parent, descendants auto-rebase
- **First-class conflicts**: Conflicts are stored in commits, resolve when convenient
- **Operation log**: Every operation can be undone with `jj undo`
- **Change IDs**: Stable identifiers that survive rebases (unlike git commit hashes)

### Editor Configuration

For non-interactive operation (no editor popups):

```bash
jj config set --user ui.editor "true"
```

### Key jj Concepts

```bash
# Bookmarks = Git branches
jj bookmark list                    # List all bookmarks

# Working copy IS a commit
jj status                           # See current state
jj diff                             # See changes in working copy

# Change IDs vs Commit IDs
# - Change ID (e.g., nrwuorzn): stable across rewrites
# - Commit ID (e.g., 2f33d1d7): changes when commit is modified
```

## Updating from Upstream

When upstream releases a new version:

### Step 1: Fetch upstream changes

```bash
git fetch author
jj git import
```

### Step 2: Update main

```bash
git checkout main
git merge author/main --ff-only
jj git import
```

### Step 3: Rebase the entire feature chain onto new main

```bash
jj rebase -s 'roots(::alberto/my-bitmagnet ~ ::main)' -d main
```

### Step 4: Resolve any conflicts

```bash
# Check for conflicts
jj log -r 'conflicts()'

# For each conflicted commit:
jj new <conflicted-change-id>    # Work on top of conflict
# Edit files to resolve (or restore build artifacts from main)
jj restore --from main webui/dist/bitmagnet/browser/index.html
jj squash                         # Move resolution into parent
```

### Step 5: Rebuild webui and finalize

```bash
# Edit the integration branch
jj edit alberto/my-bitmagnet

# Rebuild webui with all features
task build-webui

# Create new working copy
jj new

# Sync git
git checkout alberto/my-bitmagnet
```

## Adding a New Feature

### Option 1: Extend the Stack (feature depends on previous)

Add a new feature to the chain before my-bitmagnet:

```bash
# Insert new feature before my-bitmagnet
jj new alberto/search-filters-published-at -m "feat: new feature"
jj bookmark create alberto/new-feature

# Rebase my-bitmagnet on top
jj rebase -r alberto/my-bitmagnet -d alberto/new-feature

# Rebuild webui
jj edit alberto/my-bitmagnet
task build-webui
jj new
```

### Option 2: Create Parallel Branch (independent feature)

For features that don't depend on others:

```bash
# Create from main
jj new main -m "feat: independent feature"
jj bookmark create alberto/independent-feature

# Convert my-bitmagnet to multi-parent merge
jj abandon alberto/my-bitmagnet
jj new alberto/search-filters-published-at alberto/independent-feature \
   -m "integration: my-bitmagnet combining all custom features"
jj bookmark create alberto/my-bitmagnet

# Rebuild webui
task build-webui
jj new
```

## The Integration Branch (my-bitmagnet)

`alberto/my-bitmagnet` is the integration point where all features come together.

### What it contains:

- All parent features (size filter, published-at filter)
- Rebuilt webui with all features compiled in
- FORK_WORKFLOW.md documentation

### Building and Running

```bash
# Make sure you're on the integration branch
git checkout alberto/my-bitmagnet

# Build Go binary (includes git tag in version)
task build

# Build web UI (if needed)
task build-webui

# Run tests
task test

# Or run with Docker
docker compose up
```

## Building and Running

### Build commands

```bash
# Build Go binary (includes git tag in version)
task build

# Build web UI
task build-webui

# Run tests
task test

# Generate code (after schema changes)
task gen

# Install webui dependencies
task install-webui
```

### Docker

```bash
# Development with hot reload
docker compose up

# Build production image
docker build -t bitmagnet:my-bitmagnet .
```

### Verify installation

```bash
./bitmagnet version
# Should show version with git tag
```

## Common jj Commands

### Navigation

```bash
jj log                              # View commit graph
jj log -r 'main::alberto/my-bitmagnet'  # View feature chain
jj status                           # Current state
jj diff                             # Changes in working copy
```

### Branching

```bash
jj bookmark list                    # List bookmarks
jj bookmark create <name>           # Create at current commit
jj bookmark set <name> -r <rev>     # Move bookmark
jj bookmark set <name> --allow-backwards  # Move bookmark backward
```

### Editing history

```bash
jj edit <change-id>                 # Edit existing commit
jj new                              # Create new commit
jj new -m "message"                 # Create with message
jj squash                           # Move changes to parent
jj describe -m "message"            # Change commit message
jj abandon <change-id>              # Remove commit
```

### Rebasing

```bash
jj rebase -d main                   # Rebase current onto main
jj rebase -s <rev> -d <dest>        # Rebase rev and descendants
jj rebase -r <rev> -d <dest>        # Rebase only rev (not descendants)
```

### Syncing with Git

```bash
jj git fetch                        # Fetch from remote
jj git import                       # Import git changes to jj
jj git push --tracked               # Push tracked bookmarks
jj git push --bookmark <name>       # Push specific bookmark
```

### Undo mistakes

```bash
jj undo                             # Undo last operation
jj op log                           # View operation history
jj op restore <op-id>               # Restore to specific state
```

## Workflow Tips

### Always check status after operations

```bash
jj status && git status
```

### View what will be pushed

```bash
jj git push --dry-run --tracked
```

### Resolve conflicts in order

When rebasing creates conflicts in multiple commits:

1. Find all conflicts: `jj log -r 'conflicts()'`
2. Resolve parent commits first
3. Child commits may auto-resolve when parents are fixed

### Quick reference for common tasks

```bash
# View entire feature chain
jj log -r 'main::alberto/my-bitmagnet'

# Update from upstream
git fetch author && git checkout main && git merge author/main --ff-only
jj git import
jj rebase -s 'roots(::alberto/my-bitmagnet ~ ::main)' -d main

# After resolving conflicts, rebuild webui
jj edit alberto/my-bitmagnet
task build-webui
jj new

# Push changes
jj git push --tracked
```

## File Locations

| File                 | Purpose                                |
| -------------------- | -------------------------------------- |
| `Taskfile.yml`       | Build and development tasks            |
| `docker-compose.yml` | Docker development environment         |
| `Dockerfile`         | Production container build             |
| `internal/`          | Go backend code                        |
| `webui/`             | Frontend application                   |
| `webui/dist/`        | Built frontend (rebuild after changes) |

---

_Last updated: 2026-01-14_
_Rebased to upstream main (2b9e8ead - July 2025)_
