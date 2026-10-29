#!/usr/bin/env bash
set -Eeuo pipefail
script=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")/update-server.sh")
test_dir=$(mktemp -d /tmp/scs-git-compat.XXXXXX)
cleanup_test() {
  local resolved
  resolved=$(readlink -f "$test_dir")
  [[ $resolved == /tmp/scs-git-compat.* && $resolved == "$test_dir" ]] || return 1
  rm -rf -- "$resolved"
}
trap cleanup_test EXIT
repo="$test_dir/repo with spaces"
mkdir "$repo"
(cd "$repo" && git init -q && git symbolic-ref HEAD refs/heads/master)
# Model the user's old Git while retaining real repository/branch/status behavior.
git() {
  if [[ ${1:-} == -C ]]; then printf 'Unknown option: -C\n' >&2; return 129; fi
  if [[ ${TEST_FAIL_STATUS:-0} == 1 && ${1:-} == status ]]; then return 128; fi
  command git "$@"
}
docker() { printf 'REACHED_DOCKER_PREFLIGHT\n'; return 73; }
export -f git docker
status=0
bash -c 'source "$1"; main --repo "$2" --backup-dir "$3"' \
  _ "$script" "$repo" "$test_dir/backups" >"$test_dir/result.log" 2>&1 || status=$?
if [[ $status != 73 ]]; then
  cat "$test_dir/result.log"
  printf 'FAIL: master 仓库应通过 Git 检查后到达 Docker 检查，实际退出码 %s\n' "$status" >&2
  exit 1
fi
! grep -q 'Unknown option: -C' "$test_dir/result.log"
grep -q REACHED_DOCKER_PREFLIGHT "$test_dir/result.log"
printf 'PASS: 不支持 -C 的 Git 可通过真实 master/工作区检查，含空格目录也可用\n'
status=0
TEST_FAIL_STATUS=1 bash -c 'source "$1"; main --repo "$2" --backup-dir "$3"' \
  _ "$script" "$repo" "$test_dir/backups" >"$test_dir/result.log" 2>&1 || status=$?
[[ $status == 1 ]]
grep -q '无法读取 Git 工作区状态' "$test_dir/result.log"
! grep -q REACHED_DOCKER_PREFLIGHT "$test_dir/result.log"
printf 'PASS: Git 状态读取失败时停止，不误判为干净工作区\n'
