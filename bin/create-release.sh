#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
cd "$SCRIPT_DIR/.."

version="${1:-}"
if [[ -z $version ]]; then
  echo "USAGE: $0 version" >&2
  exit 1
fi

# Strip "v" prefix if provided
version="${version#v}"

if [[ "$(git symbolic-ref --short HEAD)" != "main" ]]; then
  echo "must be on main branch" >&2
  exit 1
fi

waitForPr() {
  local pr=$1
  while true; do
    if gh pr view "$pr" | grep -q 'MERGED'; then
      break
    fi
    echo "Waiting for PR to be merged..."
    sleep 5
  done
}

# ensure we are up-to-date
uncommitted_changes=$(git diff --compact-summary)
if [[ -n $uncommitted_changes ]]; then
  echo -e "There are uncommitted changes, exiting:\n${uncommitted_changes}" >&2
  exit 1
fi
git pull git@github.com:Mic92/nix-grpc-store main
unpushed_commits=$(git log --format=oneline origin/main..main)
if [[ $unpushed_commits != "" ]]; then
  echo -e "\nThere are unpushed changes, exiting:\n$unpushed_commits" >&2
  exit 1
fi
# make sure tag does not exist
if git tag -l | grep -q "^v${version}\$"; then
  echo "Tag v${version} already exists, exiting" >&2
  exit 1
fi

# The plugin, the daemon and the images all take their version from here.
echo "${version}" > .version
# Chart version and appVersion follow the tag, image tags default to appVersion.
sed -i -e "s!^version: .*\$!version: ${version}!" \
  -e "s!^appVersion: .*\$!appVersion: \"${version}\"!" \
  deploy/helm/nix-grpc-farm/Chart.yaml

git add .version deploy/helm/nix-grpc-farm/Chart.yaml
git branch -D "release-${version}" || true
git checkout -b "release-${version}"
git commit -m "bump version ${version}"
git push origin "release-${version}"
pr_url=$(gh pr create \
  --base main \
  --head "release-${version}" \
  --title "Release ${version}" \
  --body "Release ${version} of nix-grpc-store")

# Extract PR number from URL
pr_number=$(echo "$pr_url" | grep -oE '[0-9]+$')

# Enable auto-merge with specific merge method and delete branch
gh pr merge "$pr_number" --auto --merge --delete-branch
git checkout main

waitForPr "release-${version}"
git pull git@github.com:Mic92/nix-grpc-store main
gh release create "v${version}" --draft --title "v${version}" --notes ""
