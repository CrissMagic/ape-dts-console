#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/update-server.sh"
root=$(mktemp -d /tmp/scs-resume-test.XXXXXX)
cleanup_test() {
  local resolved
  resolved=$(readlink -f "$root")
  [[ $resolved == /tmp/scs-resume-test.* && $resolved == "$root" ]] || return 1
  rm -rf -- "$resolved"
}
trap cleanup_test EXIT
json_node() { node -e "$json_program" -- scs-update-json "$@"; }
container=platform
http_port=18088
ready_timeout=1
json_image=unused
test_running=false
test_id=$(printf 'a%.0s' {1..64})
test_exit=0
test_attach_status=0
test_upgrade_output=''
prepare() {
  case_dir=$root/$1
  mkdir -p "$case_dir/20261015-110123-bbbbbbbbbbbb"
  checkpoint=$case_dir/20261015-110123-bbbbbbbbbbbb
  printf 'STOP_PLATFORM_BEFORE_SECURITY_MIGRATION\n' >"$checkpoint/upgrade.log"
  touch "$checkpoint/backup-complete" "$checkpoint/migration-attempted"
  printf 'dump' >"$checkpoint/platform.dump"
  printf 'storage' >"$checkpoint/storage.tar.gz"
  printf '%s\n%s\n' bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb scs-data-sync-platform:bbbbbbbbbbbb >"$checkpoint/new-version.txt"
  node -e '
    require("node:fs").writeFileSync(process.argv[1],JSON.stringify([{
      Id:"a".repeat(64),Name:"/platform",Config:{Env:[],User:"10001:10001"},
      HostConfig:{NetworkMode:"bridge",PortBindings:{"3000/tcp":[{HostIp:"127.0.0.1",HostPort:"18080"}]}}
    }]));
  ' "$checkpoint/old-container.json"
  backup_dir='' maintenance='' old_name='' work_dir=''
  migration_attempted=0 new_created=0 stopped=0 renamed=0
  : >"$case_dir/commands"
}
docker() {
  printf '%s\n' "$*" >>"$case_dir/commands"
  if [[ $1 == inspect && ${2:-} == --format ]]; then
    if [[ $3 == '{{.Id}} {{.State.Running}}' ]]; then printf '%s %s\n' "$test_id" "$test_running";
    else printf '%s\n' "$test_exit"; fi
  elif [[ $1 == inspect ]]; then return 1;
  elif [[ $1 == start && ${2:-} == --attach ]]; then
    printf '%s' "$test_upgrade_output"
    return "$test_attach_status"
  fi
}
create_container() {
  printf 'CREATE %s %s\n' "$1" "$2" >>"$case_dir/commands"
  payload "$2" >"$case_dir/payload.json"
}
ready() { return 0; }
attempt() {
  status=0
  (trap cleanup EXIT; resume_update "$checkpoint") >"$case_dir/output" 2>&1 || status=$?
}
prepare success
attempt
[[ $status == 0 && -e $checkpoint/update-complete ]]
grep -q '^CREATE platform false$' "$case_dir/commands"
grep -q '^rm platform-previous-20261015-110123$' "$case_dir/commands"
! grep -q '^start platform-previous' "$case_dir/commands"
node -e '
  const p=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
  require("node:assert/strict").deepEqual(p.HostConfig.PortBindings["3000/tcp"],[{HostIp:"127.0.0.1",HostPort:"18088"}]);
' "$case_dir/payload.json"
printf 'PASS: 从锁冲突断点续跑到 READY，使用 18088，不启动旧版本\n'
prepare structured_diagnostic
printf '%s\n' '{"event":"PLATFORM_CONTROLLER_LOCK_TIMEOUT","waitSeconds":60,"diagnosticsAvailable":true,"holders":[{"pid":30242,"client_addr":"192.0.2.10"}]}' \
  'STOP_PLATFORM_BEFORE_SECURITY_MIGRATION' >"$checkpoint/upgrade.log"
attempt
[[ $status == 0 && -e $checkpoint/update-complete ]]
printf 'PASS: 兼容带持锁连接诊断的新日志续跑\n'
prepare mixed_error
printf '%s\n' '{"event":"PLATFORM_CONTROLLER_LOCK_TIMEOUT","waitSeconds":60,"diagnosticsAvailable":true,"holders":[]}' \
  'SECURITY_COMMAND_FAILED' 'STOP_PLATFORM_BEFORE_SECURITY_MIGRATION' >"$checkpoint/upgrade.log"
attempt
[[ $status == 1 && ! -s $case_dir/commands ]]
printf 'PASS: 拒绝混入其他失败的日志，不能仅凭末行放行\n'
prepare attach_lock_failure
test_attach_status=1
test_upgrade_output=$'{"event":"PLATFORM_CONTROLLER_LOCK_TIMEOUT","waitSeconds":60,"diagnosticsAvailable":false,"holders":[]}\nSTOP_PLATFORM_BEFORE_SECURITY_MIGRATION\n'
attempt
[[ $status == 1 && ! -e $checkpoint/update-complete ]]
grep -q 'PLATFORM_CONTROLLER_LOCK_TIMEOUT' "$case_dir/output"
grep -q '本次尚未调用迁移' "$case_dir/output"
grep -q -- '--resume-backup' "$case_dir/output"
! grep -q '已尝试迁移，禁止仅切回旧镜像' "$case_dir/output"
! grep -q '^CREATE platform false$' "$case_dir/commands"
! grep -q '^start platform-previous' "$case_dir/commands"
test_attach_status=0
test_upgrade_output=''
printf 'PASS: Docker attach 失败也输出诊断，保留备份及停止状态，不误称已迁移\n'
prepare incomplete
rm "$checkpoint/backup-complete"
attempt
[[ $status == 1 && ! -s $case_dir/commands ]]
printf 'PASS: 拒绝不完整备份\n'
prepare uncertain
printf 'SECURITY_COMMAND_FAILED\n' >"$checkpoint/upgrade.log"
attempt
[[ $status == 1 && ! -s $case_dir/commands ]]
printf 'PASS: 拒绝数据库状态不明确的其他迁移失败\n'
prepare running
test_running=true
attempt
[[ $status == 1 ]]
! grep -q '^CREATE' "$case_dir/commands"
test_running=false
printf 'PASS: 拒绝原容器仍运行的续跑\n'
prepare different
saved_id=$test_id
test_id=wrong
attempt
[[ $status == 1 ]]
! grep -q '^CREATE' "$case_dir/commands"
test_id=$saved_id
printf 'PASS: 拒绝原容器 ID 不匹配\n'
prepare migration_failure
test_exit=1
attempt
[[ $status == 1 && ! -e $checkpoint/update-complete ]]
! grep -q '^CREATE platform false$' "$case_dir/commands"
! grep -q '^start platform-previous' "$case_dir/commands"
test_exit=0
printf 'PASS: 再次升级失败不启动新旧平台\n'
prepare readiness_failure
ready_timeout=0
attempt
[[ $status == 1 && ! -e $checkpoint/update-complete ]]
grep -q '^stop --time 60 platform$' "$case_dir/commands"
! grep -q '^rm platform-previous' "$case_dir/commands"
printf 'PASS: 新平台未就绪时停止，保留旧容器及备份\n'
printf '全部 10 组续跑行为检查通过。\n'
