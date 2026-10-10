#!/usr/bin/env bash
# Publish the current HEAD tree to a GitHub branch via GraphQL createCommitOnBranch.
#
# GitHub creates the commit server-side and signs it with the web-flow key, so the
# published commit shows as Verified (committer is typically "GitHub"). This does
# not push local commit objects or preserve multi-commit ancestry — it applies the
# file delta from the remote tip (or an explicit base) onto the branch as one commit.
#
# Usage:
#   scripts/gh-verified-push.sh [--repo owner/name] [--branch name]
#       [--base-oid sha] [--expected-oid sha] [--message "subject"]
#       [--body "body"] [--create-branch-from sha] [--dry-run] [--sync-local]
#
# Defaults:
#   --repo           gh repo view --json nameWithOwner
#   --branch         current branch short name
#   --expected-oid   current remote tip of --branch (required if branch exists)
#   --base-oid       same as --expected-oid (diff base for fileChanges)
#   --message / body last local commit subject/body
#
# New branch:
#   pass --create-branch-from <sha> (usually origin/main) to create the ref first,
#   then publish the delta from that sha to HEAD.
#
# Requires: git, gh, jq, base64
set -euo pipefail

# Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e scripts/gh-verified-push.sh (verbatim).
# Compare against the source to detect drift.

usage() {
  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

repo=""
branch=""
base_oid=""
expected_oid=""
message=""
body=""
create_branch_from=""
dry_run=false
sync_local=false
max_blob_bytes=$((5 * 1024 * 1024))

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:?}"
      shift 2
      ;;
    --branch)
      branch="${2:?}"
      shift 2
      ;;
    --base-oid)
      base_oid="${2:?}"
      shift 2
      ;;
    --expected-oid)
      expected_oid="${2:?}"
      shift 2
      ;;
    --message)
      message="${2:?}"
      shift 2
      ;;
    --body)
      body="${2:?}"
      shift 2
      ;;
    --create-branch-from)
      create_branch_from="${2:?}"
      shift 2
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    --sync-local)
      sync_local=true
      shift
      ;;
    -h | --help)
      usage 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      ;;
  esac
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd git
require_cmd gh
require_cmd jq
require_cmd base64

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not inside a git work tree." >&2
  exit 1
fi

if [ "$dry_run" = false ] && [ -n "$(git status --porcelain)" ]; then
  echo "Working tree is dirty. Commit or stash before verified push." >&2
  exit 1
fi

head_sha=$(git rev-parse HEAD)
head_tree=$(git rev-parse 'HEAD^{tree}')

if [ -z "$repo" ]; then
  repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
fi
if [ -z "$branch" ]; then
  branch=$(git rev-parse --abbrev-ref HEAD)
fi
if [ "$branch" = "HEAD" ]; then
  echo "Detached HEAD: pass --branch explicitly." >&2
  exit 1
fi
if [ -z "$message" ]; then
  message=$(git log -1 --format=%s HEAD)
fi
if [ -z "$body" ]; then
  body=$(git log -1 --format=%b HEAD)
fi

remote_tip=$(git ls-remote "https://github.com/${repo}.git" "refs/heads/${branch}" 2>/dev/null | awk '{print $1}' || true)

if [ -n "$create_branch_from" ]; then
  create_branch_from=$(git rev-parse "${create_branch_from}^{commit}")
  if [ -n "$remote_tip" ]; then
    echo "Branch ${branch} already exists at ${remote_tip}; refuse --create-branch-from." >&2
    exit 1
  fi
  if [ -z "$expected_oid" ]; then
    expected_oid="$create_branch_from"
  fi
  if [ -z "$base_oid" ]; then
    base_oid="$create_branch_from"
  fi
elif [ -z "$expected_oid" ]; then
  if [ -z "$remote_tip" ]; then
    echo "Remote branch ${branch} not found on ${repo}." >&2
    echo "Pass --create-branch-from <base-sha> (for example origin/main) to create it." >&2
    exit 1
  fi
  expected_oid="$remote_tip"
fi

if [ -z "$base_oid" ]; then
  base_oid="$expected_oid"
fi

base_oid=$(git rev-parse "${base_oid}^{commit}")
expected_oid=$(git rev-parse "${expected_oid}^{commit}")

if ! git cat-file -e "${base_oid}^{commit}" 2>/dev/null; then
  echo "Base oid ${base_oid} is not available locally. Fetch first." >&2
  exit 1
fi

if ! git merge-base --is-ancestor "$base_oid" HEAD; then
  echo "Base ${base_oid} is not an ancestor of HEAD; refuse rewritten history." >&2
  exit 1
fi

base_tree=$(git rev-parse "${base_oid}^{tree}")
if [ "$base_tree" = "$head_tree" ]; then
  echo "No tree changes between ${base_oid:0:7} and HEAD; nothing to publish." >&2
  exit 0
fi

merge_commit=$(git rev-list --min-parents=2 --max-count=1 "${base_oid}..HEAD" || true)
if [ -n "$merge_commit" ]; then
  echo "Range ${base_oid:0:7}..HEAD contains merge commit ${merge_commit:0:7}." >&2
  echo "GraphQL push cannot preserve merge ancestry; squash or rebase first." >&2
  exit 1
fi

build_additions() {
  local added_files first fpath tree_entry file_mode file_type file_oid blob_size b64
  added_files=$(git diff --no-renames --name-only --diff-filter=AM "$base_oid" HEAD)
  if [ -z "$added_files" ]; then
    printf '[]'
    return 0
  fi
  first=true
  printf '['
  while IFS= read -r fpath; do
    [ -n "$fpath" ] || continue
    tree_entry=$(git ls-tree HEAD -- "$fpath")
    if [ -z "$tree_entry" ]; then
      echo "Could not resolve path in HEAD tree: $fpath" >&2
      return 1
    fi
    file_mode=$(printf '%s\n' "$tree_entry" | awk '{print $1}')
    file_type=$(printf '%s\n' "$tree_entry" | awk '{print $2}')
    file_oid=$(printf '%s\n' "$tree_entry" | awk '{print $3}')
    if [ "$file_type" != "blob" ] || [ "$file_mode" = "160000" ]; then
      echo "Only regular file blobs are supported; refusing $fpath (mode=$file_mode type=$file_type)" >&2
      return 1
    fi
    blob_size=$(git cat-file -s "$file_oid")
    if [ "$blob_size" -gt "$max_blob_bytes" ]; then
      echo "Refusing large file $fpath (${blob_size} bytes > ${max_blob_bytes})" >&2
      return 1
    fi
    b64=$(git cat-file -p "$file_oid" | base64 | tr -d '\n')
    if [ "$first" = true ]; then first=false; else printf ','; fi
    printf '{"path":%s,"contents":%s}' \
      "$(printf '%s' "$fpath" | jq -Rs .)" \
      "$(printf '%s' "$b64" | jq -Rs .)"
  done <<< "$added_files"
  printf ']'
}

build_deletions() {
  local deleted_files first fpath
  deleted_files=$(git diff --no-renames --name-only --diff-filter=D "$base_oid" HEAD)
  if [ -z "$deleted_files" ]; then
    printf '[]'
    return 0
  fi
  first=true
  printf '['
  while IFS= read -r fpath; do
    [ -n "$fpath" ] || continue
    if [ "$first" = true ]; then first=false; else printf ','; fi
    printf '{"path":%s}' "$(printf '%s' "$fpath" | jq -Rs .)"
  done <<< "$deleted_files"
  printf ']'
}

additions=$(build_additions)
deletions=$(build_deletions)

changed_count=$(
  {
    git diff --no-renames --name-only --diff-filter=AM "$base_oid" HEAD
    git diff --no-renames --name-only --diff-filter=D "$base_oid" HEAD
  } | sed '/^$/d' | wc -l | tr -d ' '
)

echo "Verified GraphQL push plan:" >&2
echo "  repo:          $repo" >&2
echo "  branch:        $branch" >&2
echo "  base:          $base_oid" >&2
echo "  expected head: $expected_oid" >&2
echo "  local HEAD:    $head_sha" >&2
echo "  local tree:    $head_tree" >&2
echo "  files changed: $changed_count" >&2
echo "  message:       $message" >&2

if [ "$dry_run" = true ]; then
  echo "Dry run only; no remote mutation." >&2
  exit 0
fi

if [ -n "$create_branch_from" ]; then
  echo "Creating branch ${branch} at ${create_branch_from}..." >&2
  gh api -X POST "repos/${repo}/git/refs" \
    -f ref="refs/heads/${branch}" \
    -f sha="$create_branch_from" >/dev/null
fi

query=$(
  cat <<'GRAPHQL'
mutation($input: CreateCommitOnBranchInput!) {
  createCommitOnBranch(input: $input) {
    commit { oid url }
  }
}
GRAPHQL
)

additions_file=$(mktemp)
deletions_file=$(mktemp)
printf '%s\n' "$additions" >"$additions_file"
printf '%s\n' "$deletions" >"$deletions_file"
cleanup() {
  rm -f "$additions_file" "$deletions_file" "${variables_file:-}"
}
trap cleanup EXIT

variables=$(
  jq -n \
    --arg nwo "$repo" \
    --arg branch "$branch" \
    --arg oid "$expected_oid" \
    --arg headline "$message" \
    --arg body "$body" \
    --slurpfile additions "$additions_file" \
    --slurpfile deletions "$deletions_file" \
    '{input: {
      branch: { repositoryNameWithOwner: $nwo, branchName: $branch },
      message: { headline: $headline, body: $body },
      fileChanges: { additions: $additions[0], deletions: $deletions[0] },
      expectedHeadOid: $oid
    }}'
)

variables_file=$(mktemp)
printf '%s\n' "$variables" >"$variables_file"
payload=$(jq -n --arg query "$query" --slurpfile variables "$variables_file" \
  '{query: $query, variables: $variables[0]}')

result=$(gh api graphql --input - <<<"$payload") || {
  echo "GraphQL createCommitOnBranch failed." >&2
  printf '%s\n' "$result" >&2
  exit 1
}

if printf '%s' "$result" | jq -e '.errors? | select(length > 0)' >/dev/null 2>&1; then
  echo "GraphQL createCommitOnBranch returned errors:" >&2
  printf '%s\n' "$result" | jq . >&2
  exit 1
fi

new_oid=$(printf '%s' "$result" | jq -r '.data.createCommitOnBranch.commit.oid // empty')
new_url=$(printf '%s' "$result" | jq -r '.data.createCommitOnBranch.commit.url // empty')
if [ -z "$new_oid" ]; then
  echo "GraphQL response missing commit oid:" >&2
  printf '%s\n' "$result" | jq . >&2
  exit 1
fi

echo "Published verified commit: $new_oid" >&2
if [ -n "$new_url" ] && [ "$new_url" != "null" ]; then
  echo "URL: $new_url" >&2
fi

# Confirm hosted blob contents match local HEAD. createCommitOnBranch does not
# preserve executable mode (100755 becomes 100644), so full tree SHAs can differ
# even when file contents are identical.
verify_json=$(gh api "repos/${repo}/git/commits/${new_oid}")
remote_tree=$(printf '%s' "$verify_json" | jq -r .tree.sha)
if [ "$remote_tree" = "$head_tree" ]; then
  echo "Hosted tree matches local HEAD tree." >&2
else
  echo "Hosted tree ${remote_tree} differs from local HEAD tree ${head_tree}; checking content..." >&2
  git fetch "https://github.com/${repo}.git" "+${new_oid}:refs/gh-verified-push/remote-tip" 2>/dev/null \
    || git fetch origin "+${new_oid}:refs/gh-verified-push/remote-tip"
  local_blobs=$(git ls-tree -r HEAD | awk '{print $3 "\t" $4}' | sort)
  remote_blobs=$(git ls-tree -r refs/gh-verified-push/remote-tip | awk '{print $3 "\t" $4}' | sort)
  if [ "$local_blobs" != "$remote_blobs" ]; then
    echo "Hosted blob contents do not match local HEAD." >&2
    diff -u <(printf '%s\n' "$local_blobs") <(printf '%s\n' "$remote_blobs") >&2 || true
    git update-ref -d refs/gh-verified-push/remote-tip 2>/dev/null || true
    exit 1
  fi
  local_modes=$(git ls-tree -r HEAD | awk '{print $1 "\t" $4}' | sort)
  remote_modes=$(git ls-tree -r refs/gh-verified-push/remote-tip | awk '{print $1 "\t" $4}' | sort)
  if [ "$local_modes" != "$remote_modes" ]; then
    echo "Warning: file modes differ (GraphQL createCommitOnBranch stores blobs as 100644)." >&2
    diff -u <(printf '%s\n' "$local_modes") <(printf '%s\n' "$remote_modes") >&2 || true
  fi
  git update-ref -d refs/gh-verified-push/remote-tip 2>/dev/null || true
  echo "Hosted blob contents match local HEAD." >&2
fi

verification=$(gh api "repos/${repo}/commits/${new_oid}" --jq '.commit.verification.verified // false')
echo "GitHub verification.verified=${verification}" >&2
if [ "$verification" != "true" ]; then
  echo "Published commit is not verified on GitHub." >&2
  exit 1
fi

if [ "$sync_local" = true ]; then
  echo "Syncing local branch to published commit ${new_oid}..." >&2
  git fetch "https://github.com/${repo}.git" "+${new_oid}:refs/gh-verified-push/tmp" 2>/dev/null \
    || git fetch origin "+${new_oid}:refs/gh-verified-push/tmp"
  git reset --hard "$new_oid"
  git update-ref -d refs/gh-verified-push/tmp 2>/dev/null || true
fi

printf '%s\n' "$new_oid"
