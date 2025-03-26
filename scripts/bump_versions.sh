#!/usr/bin/env bash
#
# Increment patch versions for crates/packages whose name starts with "alto-"
# in both package declarations and [workspace.dependencies].

set -euo pipefail

# Function: bump the patch number in e.g., 0.0.14 -> 0.0.15
bump_version() {
  local old="$1"
  local major minor patch
  IFS='.' read -r major minor patch <<< "${old}"
  patch=$((patch + 1))
  echo "${major}.${minor}.${patch}"
}

# Recursively find all Cargo.toml files
find . -name "Cargo.toml" | while read -r cargo_file; do
  # We'll store updated file content in an array
  content=()
  changed=false

  # Read the file line by line
  name=""
  while IFS= read -r line; do
    # 1) Match workspace deps like: alto-foo = { version = "0.0.3", path = "foo" }
    if [[ "${line}" =~ ^[[:space:]]*(alto-[^[:space:]]+)[[:space:]]*=[[:space:]]*\{[[:space:]]*version[[:space:]]*=[[:space:]]*\"([0-9]+\.[0-9]+\.[0-9]+)\" ]]; then
      old="${BASH_REMATCH[2]}"
      new="$(bump_version "${old}")"
      line="${line/${old}/${new}}"
      changed=true
    fi

    # 2) Check for package name lines like: name = "alto-foo"
    if [[ "${line}" =~ ^[[:space:]]*name[[:space:]]*=[[:space:]]*\"(alto-[^\"]+)\" ]]; then
      name="${BASH_REMATCH[1]}"
    else
      # 3) If name is set, we may be on a version line
      if [[ -n "${name}" && "${line}" =~ ^[[:space:]]*version[[:space:]]*=[[:space:]]*\"([0-9]+\.[0-9]+\.[0-9]+)\" ]]; then
        old="${BASH_REMATCH[1]}"
        new="$(bump_version "${old}")"
        line="${line/${old}/${new}}"
        changed=true
        name=""
      fi
    fi

    content+=("${line}")
  done < "${cargo_file}"

  # If we changed anything, overwrite the file
  if ${changed}; then
    for line in "${content[@]}"; do
      printf "%s\n" "${line}"
    done > "${cargo_file}"
    echo "Updated ${cargo_file}"
  fi
done

# Handle explorer/package.json
if [ -f "explorer/package.json" ]; then
  # Create a temporary file
  temp_file=$(mktemp)
  changed=false

  # Read the file line by line and process it directly
  while IFS= read -r line || [ -n "$line" ]; do  # The -n "$line" part handles the last line if it doesn't end with newline
    # Look directly for the version line
    if [[ "${line}" =~ ^[[:space:]]*\"version\":[[:space:]]*\"([0-9]+\.[0-9]+\.[0-9]+)\".*$ ]]; then
      old="${BASH_REMATCH[1]}"
      new="$(bump_version "${old}")"
      line="${line/${old}/${new}}"
      changed=true
    fi
    # Write to temp file, preserving line endings
    echo "$line" >> "$temp_file"
  done < "explorer/package.json"

  # If we changed anything, replace the original file
  if ${changed}; then
    mv "$temp_file" "explorer/package.json"
    echo "Updated explorer/package.json"
  else
    rm "$temp_file"
  fi
fi
