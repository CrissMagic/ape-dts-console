#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/update-server.sh"
root=$(mktemp -d /tmp/scs-image-test.XXXXXX)
cleanup_test() {
  local resolved
  resolved=$(readlink -f "$root")
  [[ $resolved == /tmp/scs-image-test.* && $resolved == "$root" ]] || return 1
  rm -rf -- "$resolved"
}
trap cleanup_test EXIT
json_node() { node -e "$json_program" -- scs-update-json "$@"; }
container=platform
version=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
image=scs-data-sync-platform:bbbbbbbbbbbb
current_id=sha256:$(printf 'a%.0s' {1..64})
old_id=sha256:$(printf 'b%.0s' {1..64})
retagged_id=sha256:$(printf '5%.0s' {1..64})
prepare() {
  case_dir=$root/$1
  backup_dir=$case_dir/20261015-110123-bbbbbbbbbbbb
  work_dir=$case_dir/work
  mkdir -p "$backup_dir" "$work_dir"
  printf dump >"$backup_dir/platform.dump"
  printf storage >"$backup_dir/storage.tar.gz"
  printf 'STOP_PLATFORM_BEFORE_SECURITY_MIGRATION\n' >"$backup_dir/upgrade.log"
  printf '%s\n%s\n' "$version" "$image" >"$backup_dir/new-version.txt"
  touch "$backup_dir/backup-complete" "$backup_dir/migration-attempted"
  node -e '
    const fs=require("node:fs"), dir=process.argv[1], id=c=>"sha256:"+c.repeat(64);
    fs.writeFileSync(dir+"/images.json",JSON.stringify([
      ["a",["scs-data-sync-platform:bbbbbbbbbbbb"],true],
      ["b",["scs-data-sync-platform:old"]], ["c",["scs-data-sync-platform:unused"]],
      ["d",["other-business:latest"]], ["e",["scs-data-sync-platform:shared","other-business:shared"]],
      ["f",["scs-data-sync-platform:referenced"]], ["1",[],true], ["2",[]],
      ["3",["scs-data-sync-platform:failed-update"]],
      ["4",["scs-data-sync-platform:foreign-digest"],false,["other-business@sha256:"+"4".repeat(64)]],
      ["5",[]], ["6",["scs-data-sync-platform:one","scs-data-sync-platform:two"]]
    ].map(([c,tags,label,digests=[]])=>({Id:id(c),RepoTags:tags,RepoDigests:digests,
      Config:{Labels:label?{"io.scs-data-sync-platform.image":"true"}:{}}}))));
    fs.writeFileSync(dir+"/20261015-110123-bbbbbbbbbbbb/old-container.json",JSON.stringify([{
      Id:"7".repeat(64),Name:"/platform",Image:id("b"),Config:{Image:"scs-data-sync-platform:old",Env:[]},
      HostConfig:{NetworkMode:"bridge"}
    }]));
  ' "$case_dir"
  old_name=platform-previous-20261015-110123
  maintenance=platform-upgrade-test
  keep_old_images=0 ready_timeout=1 migration_attempted=0 new_created=0 stopped=0 renamed=0
  old_removed=0 migration_exit=0 ready_exit=0 remove_old_fail=0 list_fail=0 image_rm_fail=0
  : >"$case_dir/commands"
}
docker() {
  printf '%s\n' "$*" >>"$case_dir/commands"
  case "$1 ${2:-}" in
    'image ls')
      (( list_fail == 0 )) || return 1
      node -e '
        const all=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
        const label=process.argv[2].startsWith("label=");
        for(const i of all) if(label?i.Config.Labels["io.scs-data-sync-platform.image"]==="true":
          i.RepoTags.some(t=>t.startsWith("scs-data-sync-platform:"))) console.log(i.Id);
      ' "$case_dir/images.json" "${!#}";;
    'image inspect')
      node -e '
        const all=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
        const found=all.find(i=>i.Id===process.argv[2]||i.RepoTags.includes(process.argv[2]));
        if(!found) process.exit(1); console.log(JSON.stringify([found]));
      ' "$case_dir/images.json" "${!#}";;
    'image rm') return "$image_rm_fail";;
    'container ls')
      printf '%s\n' platform worker failed-previous
      if (( old_removed == 0 )); then printf '%s\n' "$old_name"; fi;;
    'inspect --format')
      case "$3" in
        '{{.State.ExitCode}}') printf '%s\n' "$migration_exit";;
        '{{.Id}} {{.State.Running}}') printf '%s false\n' "$(printf '7%.0s' {1..64})";;
        '{{.Image}}')
          case "$4" in
            platform) printf '%s\n' "$current_id";;
            worker) printf 'sha256:%s\n' "$(printf 'f%.0s' {1..64})";;
            failed-previous) printf 'sha256:%s\n' "$(printf '3%.0s' {1..64})";;
            "$old_name") printf '%s\n' "$old_id";;
            *) return 1;;
          esac;;
        *) return 1;;
      esac;;
    'inspect '*) return 1;;
    'rm '*)
      if [[ $2 == "$old_name" ]]; then
        (( remove_old_fail == 0 )) || return 1
        old_removed=1
      fi;;
  esac
}
create_container() { printf 'CREATE %s %s\n' "$1" "$2" >>"$case_dir/commands"; }
ready() { return "$ready_exit"; }
attempt() {
  status=0
  (finish_update) >"$case_dir/output" 2>&1 || status=$?
}
assert_kept() { ! grep -Fq -- "$1" < <(grep '^image rm ' "$case_dir/commands" || true); }
prepare default
attempt
[[ $status == 0 && -e $backup_dir/update-complete ]]
grep -qx 'image rm --no-prune scs-data-sync-platform:old' "$case_dir/commands"
grep -qx 'image rm --no-prune scs-data-sync-platform:unused' "$case_dir/commands"
grep -qx "image rm --no-prune sha256:$(printf '1%.0s' {1..64})" "$case_dir/commands"
grep -qx 'image rm --no-prune scs-data-sync-platform:one scs-data-sync-platform:two' "$case_dir/commands"
for kept in "$current_id" "$image" other-business shared referenced failed-update foreign-digest \
  "sha256:$(printf '2%.0s' {1..64})"; do assert_kept "$kept"; done
! grep -Eq '^(image prune|system prune|image rm .*--force|image rm .*-f |rm .*--volumes)' "$case_dir/commands"
printf 'PASS: 默认清理旧镜像和本项目 dangling，保留当前、其他业务及全部容器引用\n'
prepare keep
keep_old_images=1
attempt
[[ $status == 0 && -e $backup_dir/update-complete ]]
! grep -q '^image rm ' "$case_dir/commands"
printf 'PASS: --keep-old-images 保留旧镜像\n'
prepare retagged
node -e '
  const fs=require("node:fs"), p=process.argv[1], all=JSON.parse(fs.readFileSync(p));
  all.find(i=>i.Id.endsWith("5".repeat(64))).RepoTags=["scs-data-sync-platform:rebuild"];
  fs.writeFileSync(p,JSON.stringify(all));
' "$case_dir/images.json"
project_image_ids >"$backup_dir/project-images.txt"
node -e '
  const fs=require("node:fs"), p=process.argv[1], all=JSON.parse(fs.readFileSync(p));
  all.find(i=>i.Id.endsWith("5".repeat(64))).RepoTags=[];
  fs.writeFileSync(p,JSON.stringify(all));
' "$case_dir/images.json"
attempt
[[ $status == 0 ]]
grep -qx "image rm --no-prune $retagged_id" "$case_dir/commands"
printf 'PASS: 构建前记录归属，清理同 tag 重建后无 label 的旧 ID\n'
prepare old_container_kept
remove_old_fail=1
attempt
[[ $status == 0 ]]
assert_kept scs-data-sync-platform:old
printf 'PASS: 旧容器移除失败时保留其镜像，不强删\n'
for failure in migration readiness incomplete_backup; do
  prepare "$failure"
  case "$failure" in
    migration) migration_exit=1;;
    readiness) ready_timeout=0;;
    incomplete_backup) rm "$backup_dir/backup-complete";;
  esac
  attempt
  ! grep -q '^image rm ' "$case_dir/commands"
  if [[ $failure != incomplete_backup ]]; then [[ $status != 0 ]]; fi
done
printf 'PASS: 迁移失败、READY 失败或备份不完整时不删除镜像\n'
prepare enumeration_failure
list_fail=1
attempt
[[ $status == 0 && -e $backup_dir/update-complete ]]
! grep -q '^image rm ' "$case_dir/commands"
grep -q '镜像清理未完成' "$case_dir/output"
prepare deletion_failure
image_rm_fail=1
attempt
[[ $status == 0 && -e $backup_dir/update-complete ]]
grep -q '镜像清理未完成' "$case_dir/output"
printf 'PASS: Docker 清理失败只报告警告，不把已就绪更新误判为失败\n'
for keep in 0 1; do
  prepare "resume_$keep"
  printf '%s\n' "$retagged_id" >"$backup_dir/project-images.txt"
  keep_old_images=$keep
  status=0
  (resume_update "$backup_dir") >"$case_dir/output" 2>&1 || status=$?
  [[ $status == 0 && -e $backup_dir/update-complete ]]
  if (( keep )); then ! grep -q '^image rm ' "$case_dir/commands";
  else
    grep -qx 'image rm --no-prune scs-data-sync-platform:old' "$case_dir/commands"
    grep -qx "image rm --no-prune $retagged_id" "$case_dir/commands"
  fi
done
printf 'PASS: 断点续跑采用相同的默认清理及显式保留策略\n'
prepare legacy_resume_retagged
node -e '
  const fs=require("node:fs"), p=process.argv[1], all=JSON.parse(fs.readFileSync(p));
  all.find(i=>i.Id.endsWith("b".repeat(64))).RepoTags=[];
  fs.writeFileSync(p,JSON.stringify(all));
' "$case_dir/images.json"
status=0
(resume_update "$backup_dir") >"$case_dir/output" 2>&1 || status=$?
[[ $status == 0 && -e $backup_dir/update-complete ]]
grep -qx "image rm --no-prune $old_id" "$case_dir/commands"
printf 'PASS: 旧备份无镜像列表时仍可识别原平台因同 tag 重建而 dangling 的镜像\n'
main --keep-old-images --help >"$case_dir/help"
grep -q -- '--keep-old-images' "$case_dir/help"
printf '全部镜像清理行为检查通过。\n'
