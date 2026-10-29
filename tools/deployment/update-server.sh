#!/usr/bin/env bash
# Direct Docker deployment only. No implicit rollback after a migration attempt.
set -Eeuo pipefail

fail() { printf '错误：%s\n' "$*" >&2; exit 1; }

git_at() {
  local directory=$1
  shift
  (cd -- "$directory" && git "$@")
}

json_program='
const fs = require("node:fs");
const [operation, image, version, maintenance, httpPort] = process.argv.slice(2);
function reject(message) { const e = new Error(message); e.safe = true; throw e; }
function line(value) {
  if (typeof value !== "string" || /[\r\n\0]/.test(value)) reject("配置含无效或多行字段");
  return value;
}
try {
  const raw = fs.readFileSync(0, "utf8");
  const input = operation === "upgrade-preflight-blocked" ? raw : JSON.parse(raw);
  const old = Array.isArray(input) ? input[0] : input;
  const host = old.HostConfig;
  const networks = old.NetworkSettings?.Networks ?? {};
  switch (operation) {
    case "upgrade-preflight-blocked": {
      const lines = input.trimEnd().split(/\r?\n/);
      const code = "STOP_PLATFORM_BEFORE_SECURITY_MIGRATION";
      if (lines.length === 1 && lines[0] === code) break;
      if (lines.length !== 2 || lines[1] !== code) reject("不是明确的升级前锁冲突");
      const diagnostic = JSON.parse(lines[0]);
      if (diagnostic.event !== "PLATFORM_CONTROLLER_LOCK_TIMEOUT" || diagnostic.waitSeconds !== 60 ||
          typeof diagnostic.diagnosticsAvailable !== "boolean" || !Array.isArray(diagnostic.holders))
        reject("不是有效的控制锁诊断");
      break;
    }
    case "inspect": {
      if (!old.State.Running || host.AutoRemove || host.NetworkMode.startsWith("container:") ||
          old.Config.Labels?.["com.docker.compose.project"] || Object.keys(networks).length > 1 ||
          Object.values(networks).some(n => n.IPAMConfig && Object.keys(n.IPAMConfig).length) ||
          old.Mounts.some(m => !["bind", "tmpfs"].includes(m.Type) ||
            (m.Type === "bind" && m.RW && !["/data/storage", "/var/cache/ape-dts-platform/artifacts"].includes(m.Destination))))
        reject("不支持的部署配置，请按部署文档人工更新");
      const storage = old.Mounts.find(m => m.Destination === "/data/storage" && m.Type === "bind" && m.RW);
      if (!storage) reject("storage 必须使用可写 bind 挂载");
      const config = (old.Config.Env ?? []).filter(v => v.startsWith("SCS_PLATFORM_CONFIG_PATH=")).at(-1);
      const policy = host.RestartPolicy;
      const restart = (policy.Name || "no") + (policy.Name === "on-failure" && policy.MaximumRetryCount > 0 ? ":" + policy.MaximumRetryCount : "");
      process.stdout.write([old.Image, storage.Source, config ? config.slice(config.indexOf("=") + 1) :
        "/data/storage/.platform/config.json", restart].map(line).join("\n") + "\n");
      break;
    }
    case "command": process.stdout.write(JSON.stringify([old.Config.Entrypoint ?? null, old.Config.Cmd ?? null])); break;
    case "identity": process.stdout.write(line(old.Id) + " " + line(old.Name).replace(/^\//, "")); break;
    case "old-project-image":
      if (/^scs-data-sync-platform:[^/:\s]+$/.test(old.Config.Image ?? ""))
        process.stdout.write(line(old.Image) + "\n");
      break;
    case "image-cleanup": {
      const tags = old.RepoTags ?? [], digests = old.RepoDigests ?? [];
      if (old.Id !== version || old.Id === image || !/^sha256:[0-9a-f]{64}$/.test(old.Id) ||
          tags.some(t => !/^scs-data-sync-platform:[^/:\s]+$/.test(t)) ||
          digests.some(d => !/^scs-data-sync-platform@sha256:[0-9a-f]{64}$/.test(d))) break;
      process.stdout.write((tags.length ? tags : [old.Id]).map(line).join("\n") + "\n");
      break;
    }
    case "connection": if (!old.connection) reject("缺少数据库连接配置"); break;
    case "pg-env": {
      const c = old.connection;
      const mode = {DISABLE:"disable", REQUIRE:"require", VERIFY_FULL:"verify-full"}[c.tls.mode];
      if (!mode) reject("数据库 TLS 配置无效");
      const env = {PGHOST:c.host, PGPORT:String(c.port), PGDATABASE:c.database, PGUSER:c.username,
        PGPASSWORD:c.password, PGCONNECT_TIMEOUT:"15", PGSSLMODE:mode};
      process.stdout.write(Object.entries(env).map(([k,v]) => k + "=" + line(v)).join("\n") + "\n");
      break;
    }
    case "tls": process.stdout.write(line(old.connection.tls.mode)); break;
    case "ca": process.stdout.write(old.connection.tls.caCertificate); break;
    case "pg-args": {
      const args = ["--network", host.NetworkMode];
      for (const [field, flag] of [["ExtraHosts","--add-host"],["Dns","--dns"],
                                 ["DnsSearch","--dns-search"],["DnsOptions","--dns-option"]])
        for (const value of host[field] ?? []) args.push(flag, value);
      process.stdout.write(args.map(line).join("\0") + "\0");
      break;
    }
    case "payload": {
      const p = {};
      for (const k of ["User", "Env", "Labels", "ExposedPorts", "WorkingDir", "StopSignal", "StopTimeout"])
        if (old.Config[k] != null) p[k] = old.Config[k];
      p.Image = image; p.HostConfig = host;
      p.Env = (old.Config.Env ?? []).filter(v => !v.startsWith("SCS_PLATFORM_VERSION="));
      p.Env.push("SCS_PLATFORM_VERSION=" + version);
      host.AutoRemove = false; host.ContainerIDFile = "";
      if (httpPort) {
        if (!/^[1-9][0-9]{0,4}$/.test(httpPort) || Number(httpPort) > 65535)
          reject("HTTP 宿主端口必须为 1 到 65535");
        if (["host", "none"].includes(host.NetworkMode)) reject("当前网络模式不支持端口映射");
        const port = (old.Config.Env ?? []).findLast(v => v.startsWith("PORT="))?.slice(5) || "3000";
        if (!/^[1-9][0-9]{0,4}$/.test(port) || Number(port) > 65535) reject("容器 HTTP 端口配置无效");
        const key = port + "/tcp";
        const bindings = host.PortBindings ?? {};
        if (Object.entries(bindings).some(([k,values]) => k !== key && (values ?? []).some(v => v.HostPort === httpPort)))
          reject("目标端口已用于其他容器端口映射");
        const previous = bindings[key];
        bindings[key] = (previous?.length ? previous : [{HostIp:"127.0.0.1"}]).map(v => ({...v, HostPort:httpPort}));
        host.PortBindings = bindings;
        p.ExposedPorts = {...p.ExposedPorts, [key]:{}};
      }
      const logging = host.LogConfig ?? {};
      if (["", "json-file", "local"].includes(logging.Type ?? ""))
        host.LogConfig = {Type:logging.Type || "json-file", Config:{...logging.Config, "max-size":"10m", "max-file":"3"}};
      if (!["host", "bridge", "default", "none"].includes(host.NetworkMode)) {
        p.NetworkingConfig = {EndpointsConfig:{}};
        for (const [name,n] of Object.entries(networks))
          p.NetworkingConfig.EndpointsConfig[name] = {Aliases:(n.Aliases ?? []).filter(a => a !== old.Id && a !== old.Id.slice(0,12)), IPAMConfig:n.IPAMConfig};
      }
      if (maintenance === "true") {
        p.Entrypoint = ["node"]; p.Cmd = ["dist/bootstrap/security-cli.js", "upgrade", "--backup-confirmed"];
        host.RestartPolicy = {Name:"no", MaximumRetryCount:0}; host.PortBindings = {}; host.PublishAllPorts = false;
        p.ExposedPorts = {}; p.Healthcheck = {Test:["NONE"]};
        for (const n of Object.values(p.NetworkingConfig?.EndpointsConfig ?? {})) n.Aliases = [];
      }
      process.stdout.write(JSON.stringify(p));
      break;
    }
    default: reject("未知 JSON 操作");
  }
} catch (e) { console.error(e.safe ? e.message : "JSON 配置处理失败（" + operation + "）"); process.exitCode = 1; }
'

json_node() {
  docker run --rm -i --user 0:0 --read-only --network none \
    --log-driver=none \
    --entrypoint node "$json_image" -e "$json_program" -- scs-update-json "$@"
}

payload() {
  json_node payload "$image" "$version" "$1" "${http_port:-}" <"$backup_dir/old-container.json"
}

create_container() {
  local name=$1 maintenance=$2
  # CentOS 7 curl lacks --unix-socket; use Node already present in the built image.
  # This temporary helper alone mounts Docker's socket, never the platform workload.
  payload "$maintenance" | docker run --rm -i --user 0:0 --read-only --network none \
    --log-driver=json-file --log-opt=max-size=10m --log-opt=max-file=3 \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    --entrypoint node "$image" -e '
      const http=require("node:http"); let body="";
      process.stdin.setEncoding("utf8"); process.stdin.on("data", c=>body+=c);
      process.stdin.on("end",()=>{
        const r=http.request({socketPath:"/var/run/docker.sock",method:"POST",
          path:"/containers/create?name="+encodeURIComponent(process.argv[2]),
          headers:{"Content-Type":"application/json"}},res=>{
            res.resume(); res.on("end",()=>{if(res.statusCode!==201){
              console.error("Docker create failed: HTTP "+res.statusCode); process.exitCode=1;
            }});
          });
        r.setTimeout(30000,()=>r.destroy());
        r.on("error",()=>{console.error("Docker create failed");process.exitCode=1});r.end(body);
      });' -- scs-update-create "$name"
}

upgrade_blocked_before_migration() {
  [[ -f $backup_dir/upgrade.log ]] || return 1
  json_node upgrade-preflight-blocked <"$backup_dir/upgrade.log" >/dev/null 2>&1
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  if [[ -n ${backup_dir:-} ]]; then rm -f -- "$backup_dir/.pg-client.env"; fi
  if [[ -n ${maintenance:-} ]]; then docker rm -f "$maintenance" >/dev/null 2>&1; fi
  if (( status != 0 )); then
    printf '更新未完成。备份及诊断目录：%s\n' "${backup_dir:-尚未创建}" >&2
    if [[ ${migration_attempted:-0} == 1 ]]; then
      if [[ ${new_created:-0} == 1 ]]; then
        docker logs --tail=300 "$container" >"$backup_dir/startup.log" 2>&1
        docker update --restart=no "$container" >/dev/null
        docker stop --time 60 "$container" >/dev/null
      fi
      if [[ ${new_created:-0} == 0 ]] && upgrade_blocked_before_migration; then
        printf '控制锁尚未释放，本次尚未调用迁移；旧容器保持停止：%s\n' "$old_name" >&2
        printf '核实持锁实例后，可使用 --resume-backup %s 续跑；不会自动断开数据库连接。\n' "$backup_dir" >&2
      else
        printf '已尝试迁移，禁止仅切回旧镜像；旧容器保持停止：%s\n' "$old_name" >&2
      fi
    elif [[ ${stopped:-0} == 1 ]]; then
      if [[ ${renamed:-0} == 1 ]]; then docker rename "$old_name" "$container"; fi
      if docker update --restart="$restart" "$container" >/dev/null && docker start "$container"; then
        printf '尚未开始迁移，已重新启动原容器。\n' >&2
      else
        printf '原容器重新启动失败，请人工检查。\n' >&2
      fi
    fi
  fi
  if [[ -n ${work_dir:-} ]]; then
    # Only remove known temporary files, never recursively delete a computed directory.
    rm -f -- "$work_dir/inspect.json" "$work_dir/image.json" "$work_dir/fields" "$work_dir/pg-args" \
      "$work_dir/project-images" "$work_dir/cleanup-images" "$work_dir/cleanup-references"
    rmdir -- "$work_dir" 2>/dev/null
  fi
  exit "$status"
}

backup_database() {
  local config=$1
  local -a client=(docker run --rm --log-driver=json-file --log-opt=max-size=10m --log-opt=max-file=3)
  json_node pg-args <"$backup_dir/old-container.json" >"$work_dir/pg-args"
  while IFS= read -r -d '' item; do client+=("$item"); done <"$work_dir/pg-args"
  json_node pg-env <"$config" >"$backup_dir/.pg-client.env" 2>"$backup_dir/backup-error.log"
  local tls
  tls=$(json_node tls <"$config")
  if [[ $tls == VERIFY_FULL ]]; then
    json_node ca <"$config" >"$backup_dir/database-ca.pem"
    printf 'PGSSLROOTCERT=/backup-ca.pem\n' >>"$backup_dir/.pg-client.env"
    client+=(--mount "type=bind,src=$backup_dir/database-ca.pem,dst=/backup-ca.pem,readonly")
  fi
  "${client[@]}" --env-file "$backup_dir/.pg-client.env" --entrypoint pg_dump "$pg_client_image" \
    --format=custom --no-owner --no-acl >"$backup_dir/platform.dump" 2>>"$backup_dir/backup-error.log"
  rm -f -- "$backup_dir/.pg-client.env"
  docker run --rm -i --entrypoint pg_restore "$pg_client_image" --list \
    <"$backup_dir/platform.dump" >/dev/null 2>>"$backup_dir/backup-error.log"
}

ready() {
  docker exec "$container" node -e '
    fetch("http://127.0.0.1:"+(process.env.PORT||"3000")+"/api/health/ready",
      {signal:AbortSignal.timeout(4000)}).then(async r=>{const b=await r.json();
      process.exit(r.ok&&b.phase==="READY"?0:1)}).catch(()=>process.exit(1))
  ' >/dev/null 2>&1
}

project_image_ids() {
  docker image ls --all --quiet --no-trunc --filter 'reference=scs-data-sync-platform:*' || return 1
  docker image ls --all --quiet --no-trunc --filter 'label=io.scs-data-sync-platform.image=true'
}

cleanup_project_images() {
  [[ -f $backup_dir/backup-complete && -s $backup_dir/platform.dump &&
     -s $backup_dir/storage.tar.gz ]] || return 1
  local current_image containers reference used_image candidate listed failed=0
  local -A used=() seen=()
  current_image=$(docker inspect --format '{{.Image}}' "$container") || return 1
  [[ $current_image =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  used["$current_image"]=1
  containers=$(docker container ls --all --quiet --no-trunc) || return 1
  while IFS= read -r reference; do
    [[ -n $reference ]] || continue
    used_image=$(docker inspect --format '{{.Image}}' "$reference") || return 1
    [[ $used_image =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
    used["$used_image"]=1
  done <<<"$containers"
  project_image_ids >"$work_dir/cleanup-images" || return 1
  # A rebuild can move the same tag, leaving its previous project image unlabelled and dangling.
  if [[ -f $backup_dir/project-images.txt ]]; then
    cat -- "$backup_dir/project-images.txt" >>"$work_dir/cleanup-images" || return 1
  fi
  json_node old-project-image <"$backup_dir/old-container.json" >>"$work_dir/cleanup-images" || return 1
  local -a references
  while IFS= read -r candidate; do
    [[ $candidate =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
    [[ -z ${used[$candidate]:-} && -z ${seen[$candidate]:-} ]] || continue
    seen["$candidate"]=1
    listed=$(docker image inspect "$candidate") || return 1
    json_node image-cleanup "$current_image" "$candidate" <<<"$listed" >"$work_dir/cleanup-references" || return 1
    [[ -s $work_dir/cleanup-references ]] || continue
    mapfile -t references <"$work_dir/cleanup-references"
    if ! docker image rm --no-prune "${references[@]}"; then failed=1; fi
  done <"$work_dir/cleanup-images"
  return "$failed"
}

finish_update() {
  create_container "$maintenance" true
  migration_attempted=1
  touch "$backup_dir/migration-attempted"
  printf '检查升级条件（控制锁重试窗口 60 秒），随后升级数据库；详情见 %s/upgrade.log\n' "$backup_dir"
  local attach_status=0
  docker start --attach "$maintenance" >"$backup_dir/upgrade.log" 2>&1 || attach_status=$?
  if (( attach_status != 0 )) || [[ $(docker inspect --format '{{.State.ExitCode}}' "$maintenance") != 0 ]]; then
    tail -n 5 "$backup_dir/upgrade.log" >&2
    fail '升级命令失败，请根据上方诊断处理。'
  fi
  docker rm "$maintenance" >/dev/null
  maintenance=''
  new_created=1
  create_container "$container" false
  docker start "$container"
  local deadline=$((SECONDS + ready_timeout))
  while (( SECONDS < deadline )); do
    if ready; then
      touch "$backup_dir/update-complete"
      if docker rm "$old_name" >/dev/null; then
        printf '旧平台容器已移除。\n'
      else
        printf '新平台已就绪，但旧容器清理失败，请手动移除 %s。\n' "$old_name" >&2
      fi
      if (( ${keep_old_images:-0} )); then
        printf '按 --keep-old-images 保留旧镜像。\n'
      elif ! cleanup_project_images; then
        printf '平台已 READY，但镜像清理未完成；请检查 Docker 诊断，备份仍保留。\n' >&2
      fi
      printf '更新完成：%s，平台 READY；备份保留：%s\n' "$version" "$backup_dir"
      return
    fi
    sleep 3
  done
  fail '新平台未在等待时间内进入 READY。'
}

resume_update() {
  backup_dir=$(readlink -f -- "$1")
  [[ -d $backup_dir && -f $backup_dir/backup-complete && -f $backup_dir/migration-attempted &&
     ! -e $backup_dir/update-complete && -s $backup_dir/platform.dump && -s $backup_dir/storage.tar.gz &&
     -f $backup_dir/old-container.json && -f $backup_dir/new-version.txt ]] || fail '备份不完整或更新已完成，不能续跑。'
  # Only this diagnostic proves the previous attempt stopped before migrations were invoked.
  upgrade_blocked_before_migration ||
    fail '仅支持升级前控制锁冲突的续跑；其他失败需先人工核实数据库状态。'
  local folder=${backup_dir##*/} previous_stamp identity saved_id saved_name
  [[ $folder =~ ^([0-9]{8}-[0-9]{6})-([0-9a-f]{12})$ ]] || fail '备份目录名称无效。'
  previous_stamp=${BASH_REMATCH[1]}
  identity=$(json_node identity <"$backup_dir/old-container.json")
  read -r saved_id saved_name <<<"$identity"
  [[ $saved_name == "$container" ]] || fail '备份不属于指定平台容器。'
  old_name=$container-previous-$previous_stamp
  [[ $(docker inspect --format '{{.Id}} {{.State.Running}}' "$old_name") == "$saved_id false" ]] ||
    fail '原容器 ID 不匹配或仍在运行，不能续跑。'
  if docker inspect "$container" >/dev/null 2>&1; then fail '目标平台容器已存在，请先核实状态。'; fi
  local -a release
  mapfile -t release <"$backup_dir/new-version.txt"
  [[ ${release[0]:-} =~ ^[0-9a-f]{40}$ && ${release[1]:-} == "scs-data-sync-platform:${release[0]:0:12}" &&
     $folder == "$previous_stamp-${release[0]:0:12}" ]] || fail '备份镜像版本信息不匹配。'
  version=${release[0]}
  image=${release[1]}
  docker image inspect "$image" >/dev/null
  payload false >/dev/null
  local stamp candidate
  stamp=$(TZ=Asia/Shanghai date +%Y%m%d-%H%M%S)
  candidate=$container-upgrade-$stamp
  if docker inspect "$candidate" >/dev/null 2>&1; then fail '维护容器已存在，请先核实状态。'; fi
  cp -- "$backup_dir/upgrade.log" "$backup_dir/upgrade-before-resume-$stamp.log"
  maintenance=$candidate
  printf '复用已完成备份及镜像继续升级，不启动旧版本：%s\n' "$backup_dir"
  finish_update
}

main() {
  # xtrace would expose inherited secrets, so explicitly turn it off even under bash -x.
  set +x
  local repo=$PWD backup_root=/srv/scs-data-sync-platform/backups ready_timeout=180 http_port='' resume_backup='' keep_old_images=0
  container=scs-data-sync-platform
  pg_client_image=postgres:12-bookworm
  while (( $# )); do
    case "$1" in
      --repo|--container|--backup-dir|--pg-client-image|--ready-timeout|--http-port|--resume-backup)
        (( $# >= 2 )) || fail "缺少 $1 参数"
        case "$1" in
          --repo) repo=$2;; --container) container=$2;; --backup-dir) backup_root=$2;;
          --pg-client-image) pg_client_image=$2;; --ready-timeout) ready_timeout=$2;; --http-port) http_port=$2;;
          --resume-backup) resume_backup=$2;;
        esac
        shift 2;;
      --keep-old-images) keep_old_images=1; shift;;
      --help) printf 'Usage: bash update-server.sh [--repo DIR] [--container NAME] [--backup-dir DIR] [--pg-client-image IMAGE] [--ready-timeout SECONDS] [--http-port PORT] [--resume-backup DIR] [--keep-old-images]\n'; return;;
      *) fail "未知参数：$1";;
    esac
  done
  [[ $EUID == 0 ]] || fail '请使用 root/sudo 运行。'
  [[ $ready_timeout =~ ^[1-9][0-9]*$ ]] || fail '等待时间必须是正整数。'
  if [[ -n $http_port ]]; then
    [[ $http_port =~ ^[1-9][0-9]{0,4}$ ]] && (( http_port <= 65535 )) || fail 'HTTP 宿主端口必须为 1 到 65535。'
  fi
  [[ $container =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || fail '容器名称无效。'
  [[ -z ${DOCKER_HOST:-} && -z ${DOCKER_CONTEXT:-} ]] || fail '仅支持本机默认 Docker Socket。'
  export DOCKER_HOST=unix:///var/run/docker.sock
  umask 077
  local tool
  for tool in bash docker git tar flock readlink mktemp tail; do command -v "$tool" >/dev/null || fail "缺少工具：$tool"; done
  exec 9>/var/lock/scs-data-sync-platform-update.lock
  flock -n 9 || fail '已有更新脚本正在执行。'
  repo=$(readlink -f -- "$repo")
  backup_root=$(readlink -m -- "$backup_root")
  [[ $backup_root =~ ^/[^/]+/[^/]+ ]] || fail '必须使用专用备份子目录。'
  local branch working_changes
  branch=$(git_at "$repo" symbolic-ref --short HEAD) || fail '无法读取 Git 分支，请检查仓库或 detached HEAD 状态。'
  [[ $branch == master ]] || fail '请在 master 分支运行。'
  working_changes=$(git_at "$repo" status --porcelain) || fail '无法读取 Git 工作区状态。'
  [[ -z $working_changes ]] || fail '存在未提交改动，不会覆盖或自动 stash。'
  work_dir=$(mktemp -d)
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  json_image=node:24.19.0-bookworm-slim
  docker image inspect "$json_image" >/dev/null 2>&1 || docker pull "$json_image"
  if [[ -n $resume_backup ]]; then resume_update "$resume_backup"; return; fi
  docker inspect "$container" >"$work_dir/inspect.json"
  json_node inspect <"$work_dir/inspect.json" >"$work_dir/fields"
  local -a fields
  mapfile -t fields <"$work_dir/fields"
  docker image inspect "${fields[0]}" >"$work_dir/image.json"
  local original_command current_command
  original_command=$(json_node command <"$work_dir/image.json")
  current_command=$(json_node command <"$work_dir/inspect.json")
  [[ $original_command == "$current_command" ]] || fail '原容器覆盖了镜像启动命令。'
  # Validate a requested port change before stopping the original container.
  if [[ -n $http_port ]]; then
    json_node payload "${fields[0]}" port-check false "$http_port" <"$work_dir/inspect.json" >/dev/null
  fi
  local storage config_path config
  storage=$(readlink -f -- "${fields[1]}")
  [[ -d $storage && $storage != / ]] || fail '无效的 storage 挂载。'
  [[ $backup_root != "$storage" && $backup_root != "$storage/"* ]] || fail '备份不能放在 storage 内。'
  config_path=${fields[2]}
  [[ $config_path == /data/storage/* ]] || fail '配置文件必须位于 storage 内。'
  config=$(readlink -f -- "$storage/${config_path#/data/storage/}")
  [[ $config == "$storage/"* && -f $config ]] || fail '持久化配置不存在或越界。'
  json_node connection <"$config"
  git_at "$repo" pull --ff-only origin master
  version=$(git_at "$repo" rev-parse HEAD)
  local fetched_version
  fetched_version=$(git_at "$repo" rev-parse FETCH_HEAD)
  [[ $version == "$fetched_version" ]] || fail '存在本地未推送提交。'
  image=scs-data-sync-platform:${version:0:12}
  if (( ! keep_old_images )); then project_image_ids >"$work_dir/project-images"; fi
  printf '构建 %s，原容器暂不停机。\n' "$image"
  docker build --build-arg NODE_IMAGE=node:24.19.0-bookworm-slim \
    --label io.scs-data-sync-platform.image=true --tag "$image" "$repo"
  docker image inspect "$pg_client_image" >/dev/null 2>&1 || docker pull "$pg_client_image"
  local stamp
  stamp=$(TZ=Asia/Shanghai date +%Y%m%d-%H%M%S)
  backup_dir=$backup_root/$stamp-${version:0:12}
  mkdir -p -- "$backup_root"
  chmod 0700 "$backup_root"
  mkdir -- "$backup_dir"
  cp -- "$work_dir/inspect.json" "$backup_dir/old-container.json"
  if [[ -f $work_dir/project-images ]]; then
    cp -- "$work_dir/project-images" "$backup_dir/project-images.txt"
  fi
  printf '%s\n%s\n' "$version" "$image" >"$backup_dir/new-version.txt"
  old_name=$container-previous-$stamp
  maintenance=$container-upgrade-$stamp
  if docker inspect "$old_name" >/dev/null 2>&1 || docker inspect "$maintenance" >/dev/null 2>&1; then
    maintenance=''
    fail '同名维护/旧容器已存在，请检查上次更新状态。'
  fi
  restart=${fields[3]}
  stopped=1
  printf '停止平台并备份，任务会短暂中断。备份：%s\n' "$backup_dir"
  docker stop --time 60 "$container"
  docker update --restart=no "$container" >/dev/null
  tar --numeric-owner -czpf "$backup_dir/storage.tar.gz" -C "$storage" .
  tar -tzf "$backup_dir/storage.tar.gz" >/dev/null
  backup_database "$config"
  touch "$backup_dir/backup-complete"
  docker rename "$container" "$old_name"
  renamed=1
  finish_update
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
