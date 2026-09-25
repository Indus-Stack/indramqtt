#!/usr/bin/env bash
# Every git dependency in every Cargo.toml must name a `rev` or a `tag`.
#
# A bare `git = "..."` URL floats: the next fetch can silently change the
# code we ship. The revision above is the one the tree is tested with;
# moving it is a separate, reviewed task.
#
# Usage: pinned_git_deps.sh [path ...]
#   No args: scan the whole repository (neighbours wiring_check.sh).
#   Args: each arg is a Cargo.toml file or a directory scanned recursively
#     for Cargo.toml files. Used by the unit test with fixture files.
#
# Exit 0 prints PINNED_GIT_DEPS_OK. Exit 1 lists each offending file.
set -u

fail=0
report_offender() {
    echo "PINNED_GIT_DEPS_FAIL: $1"
    fail=1
}

# Collect the Cargo.toml files to inspect into "$@" (file list).
collect_files() {
    if [ "$#" -gt 0 ]; then
        for target in "$@"; do
            if [ -f "$target" ]; then
                printf '%s\n' "$target"
            elif [ -d "$target" ]; then
                find "$target" -name Cargo.toml -not -path '*/target/*' 2>/dev/null
            else
                report_offender "$target: path does not exist"
            fi
        done
    else
        if root=$(git rev-parse --show-toplevel 2>/dev/null); then
            :
        else
            # Not inside a git checkout (fail closed on root, keep scanning).
            root=$(dirname "$(readlink -f "$0")")/..
        fi
        find "$root" -name Cargo.toml -not -path '*/target/*' 2>/dev/null
    fi
}

# True when "$1" (an inline `{ ... }` table or a section body) pins a git
# source: it names `rev =` or `tag =` as a real key. A `branch =` alone
# still floats, so it does not count.
has_pin() {
    printf '%s' "$1" | grep -qE '(^|[{[:space:];])(rev|tag)[[:space:]]*='
}

check_file() {
    file=$1
    # Strip end-of-line comments (a space followed by `#`) so a commented
    # `# rev =` cannot pass the check; URLs carry no bare ` #`.
    body=$(sed 's/ #.*$//' "$file")
    # 1. Inline tables: `name = { git = "...", ... }`, possibly split over
    # several lines. Collapse newlines, then inspect each `{...}` span that
    # names a git source.
    collapsed=$(printf '%s' "$body" | tr '\n' ' ')
    inline_tables=$(printf '%s' "$collapsed" | grep -oE '\{[^}]*git[[:space:]]*=[^}]*\}' || true)
    if [ -n "$inline_tables" ]; then
        while IFS= read -r table; do
            [ -z "$table" ] && continue
            if ! has_pin "$table"; then
                report_offender "$file: git dependency without rev or tag: $table"
            fi
        done <<EOF
$inline_tables
EOF
    fi
    # 2. Longhand sections: `[dependencies.name]` (or dev/build/target and
    # patch sections) with a bare `git =` key. The pin must be in the same
    # section body.
    section_body=""
    section_head=""
    check_section() {
        [ -z "$section_head" ] && return 0
        case "$section_head" in
            *dependencies.*|*patch.*)
                if printf '%s' "$section_body" | grep -qE '(^|[[:space:];])git[[:space:]]*='; then
                    if ! has_pin "$section_body"; then
                        report_offender "$file: [$section_head] git dependency without rev or tag"
                    fi
                fi
                ;;
        esac
    }
    while IFS= read -r line; do
        trimmed=$(printf '%s' "$line" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        case "$trimmed" in
            \[*\]*)
                check_section
                section_head=$(printf '%s' "$trimmed" | sed 's/^\[//; s/\]$//')
                section_body=""
                ;;
            *)
                section_body=$(printf '%s\n%s' "$section_body" "$line")
                ;;
        esac
    done <<EOF
$body
EOF
    check_section
}

files=$(collect_files "$@")
if [ -z "$files" ]; then
    echo "PINNED_GIT_DEPS_OK"
    exit 0
fi
while IFS= read -r cargo_file; do
    [ -z "$cargo_file" ] && continue
    # Skip vendored snapshots and build output; they do not decide the build.
    case "$cargo_file" in
        */target/*) continue ;;
    esac
    [ -f "$cargo_file" ] || continue
    check_file "$cargo_file"
done <<EOF
$files
EOF

if [ "$fail" = 0 ]; then echo "PINNED_GIT_DEPS_OK"; fi
exit "$fail"
