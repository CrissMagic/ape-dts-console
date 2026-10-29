#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/update-server.sh"
umask 077
test_dir=$(mktemp -d)
trap 'rm -f -- "$test_dir/old-container.json" "$test_dir/payload.json" "$test_dir/commands.log" "$test_dir/.pg-client.env" "$test_dir/config.json" "$test_dir/error.log" "$test_dir/pg-args" "$test_dir/fields"; rmdir -- "$test_dir"' EXIT
backup_dir=$test_dir
image=new-image
version=new-version
# Use the exact production JSON program; only replace its Docker transport for isolated tests.
json_node() { node -e "$json_program" -- scs-update-json "$@"; }
node -e '
  require("node:fs").writeFileSync(process.argv[1], JSON.stringify([{
    Id:"aaaaaaaaaaaa", Image:"sha256:old", State:{Running:true},
    Config:{User:"10001:10001", Env:["SECRET=quote\" and space", "SCS_PLATFORM_VERSION=old"]},
    Mounts:[{Type:"bind", Source:"/srv/platform", Destination:"/data/storage", RW:true}],
    HostConfig:{NetworkMode:"platform-net", Memory:8589934592, NanoCpus:4000000000,
      AutoRemove:false, Binds:["/srv/platform:/data/storage"],
      ExtraHosts:["pg:10.0.0.1"], Dns:["10.0.0.2"], DnsSearch:["example.internal"],
      PortBindings:{"3000/tcp":[{HostIp:"127.0.0.1", HostPort:"18080"}],
                    "9090/tcp":[{HostIp:"127.0.0.1", HostPort:"19090"}]}, RestartPolicy:{Name:"unless-stopped"},
      LogConfig:{Type:"json-file", Config:{}}},
    NetworkSettings:{Networks:{"platform-net":{Aliases:["aaaaaaaaaaaa", "platform"], IPAMConfig:null}}}
  }]));
' "$test_dir/old-container.json"
json_node inspect <"$test_dir/old-container.json" >"$test_dir/fields"
mapfile -t fields <"$test_dir/fields"
[[ ${fields[0]} == sha256:old && ${fields[1]} == /srv/platform &&
   ${fields[2]} == /data/storage/.platform/config.json && ${fields[3]} == unless-stopped ]]
printf 'PASS: 读取部署配置及默认配置路径，不需要 jq\n'
payload false >"$test_dir/payload.json"
node -e '
  const assert=require("node:assert/strict"), p=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
  assert.equal(p.Image,"new-image"); assert.equal(p.User,"10001:10001");
  assert.deepEqual(p.Env,["SECRET=quote\" and space","SCS_PLATFORM_VERSION=new-version"]);
  assert.equal(p.HostConfig.Memory,8589934592); assert.equal(p.HostConfig.NanoCpus,4000000000);
  assert.deepEqual(p.HostConfig.Binds,["/srv/platform:/data/storage"]);
  assert.deepEqual(p.HostConfig.LogConfig.Config,{"max-size":"10m","max-file":"3"});
  assert.equal(p.HostConfig.PortBindings["3000/tcp"][0].HostPort,"18080");
  assert.equal(Object.hasOwn(p,"Entrypoint"),false); assert.equal(Object.hasOwn(p,"Cmd"),false);
  assert.deepEqual(p.NetworkingConfig.EndpointsConfig["platform-net"].Aliases,["platform"]);
' "$test_dir/payload.json"
printf 'PASS: 保留含引号空格的凭据、挂载、限额，并配置日志轮转\n'
http_port=18088
payload false >"$test_dir/payload.json"
node -e '
  const assert=require("node:assert/strict"), p=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
  assert.deepEqual(p.HostConfig.PortBindings["3000/tcp"],[{HostIp:"127.0.0.1",HostPort:"18088"}]);
  assert.deepEqual(p.HostConfig.PortBindings["9090/tcp"],[{HostIp:"127.0.0.1",HostPort:"19090"}]);
' "$test_dir/payload.json"
printf 'PASS: HTTP 改为 18088，保持监听 IP 及其他端口\n'
payload true >"$test_dir/payload.json"
node -e '
  const assert=require("node:assert/strict"), p=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
  assert.deepEqual(p.Entrypoint,["node"]);
  assert.deepEqual(p.Cmd,["dist/bootstrap/security-cli.js","upgrade","--backup-confirmed"]);
  assert.deepEqual(p.HostConfig.PortBindings,{}); assert.equal(p.HostConfig.RestartPolicy.Name,"no");
  assert.deepEqual(p.NetworkingConfig.EndpointsConfig["platform-net"].Aliases,[]);
' "$test_dir/payload.json"
printf 'PASS: 维护容器无端口冲突、无重启及服务别名\n'
http_port=70000
if payload false >"$test_dir/payload.json" 2>"$test_dir/error.log"; then fail '未拒绝超限端口'; fi
http_port=19090
if payload false >"$test_dir/payload.json" 2>"$test_dir/error.log"; then fail '未拒绝映射冲突'; fi
unset http_port
printf 'PASS: 拒绝超限端口及与其他映射冲突的端口\n'
node -e '
  const fs=require("node:fs"), p=process.argv[1], v=JSON.parse(fs.readFileSync(p));
  v[0].HostConfig.LogConfig={Type:"fluentd",Config:{tag:"scs"}}; fs.writeFileSync(p,JSON.stringify(v));
' "$test_dir/old-container.json"
payload false >"$test_dir/payload.json"
node -e '
  const p=JSON.parse(require("node:fs").readFileSync(process.argv[1]));
  require("node:assert/strict").deepEqual(p.HostConfig.LogConfig,{Type:"fluentd",Config:{tag:"scs"}});
' "$test_dir/payload.json"
printf 'PASS: 不替换远程日志驱动\n'
json_node pg-args <"$test_dir/old-container.json" >"$test_dir/pg-args"
node -e '
  const args=require("node:fs").readFileSync(process.argv[1],"utf8").split("\0").slice(0,-1);
  require("node:assert/strict").deepEqual(args,["--network","platform-net","--add-host","pg:10.0.0.1",
    "--dns","10.0.0.2","--dns-search","example.internal"]);
' "$test_dir/pg-args"
printf 'PASS: 数据库备份保留网络及 DNS 参数\n'
printf '%s\n' '{"connection":{"host":"pg","port":5432,"database":"platform","username":"user","password":"q\" $p=a","tls":{"mode":"VERIFY_FULL","caCertificate":"CA\nDATA"}}}' >"$test_dir/config.json"
json_node pg-env <"$test_dir/config.json" >"$test_dir/.pg-client.env"
grep -Fqx 'PGPASSWORD=q" $p=a' "$test_dir/.pg-client.env"
grep -Fqx 'PGSSLMODE=verify-full' "$test_dir/.pg-client.env"
[[ $(json_node ca <"$test_dir/config.json") == $'CA\nDATA' ]]
printf 'PASS: 数据库凭据不经 shell 求值，并保留 TLS 验证\n'
printf '%s\n' '{"connection":{"host":"pg","port":5432,"database":"platform","username":"user","password":"sensitive\ninvalid","tls":{"mode":"DISABLE"}}}' >"$test_dir/config.json"
if json_node pg-env <"$test_dir/config.json" >"$test_dir/.pg-client.env" 2>"$test_dir/error.log"; then fail '未拒绝换行密码'; fi
! grep -q sensitive "$test_dir/error.log"
[[ ! -s $test_dir/.pg-client.env ]]
printf 'PASS: 拒绝多行凭据，错误输出不泄露密码\n'
printf '%s\n' '{"secret":"sensitive"' >"$test_dir/config.json"
if json_node connection <"$test_dir/config.json" 2>"$test_dir/error.log"; then fail '未拒绝损坏 JSON'; fi
! grep -q sensitive "$test_dir/error.log"
printf 'PASS: 拒绝损坏 JSON，错误输出不泄露原文\n'

docker() { printf '%s\n' "$*" >>"$test_dir/commands.log"; }
container=platform
old_name=platform-previous
restart=unless-stopped
maintenance=''
work_dir=''
stopped=1
renamed=1
migration_attempted=0
new_created=0
if (trap cleanup EXIT; exit 42); then fail '失败状态未传递'; else [[ $? == 42 ]]; fi
grep -q '^rename platform-previous platform$' "$test_dir/commands.log"
grep -q '^start platform$' "$test_dir/commands.log"
printf 'PASS: 迁移前失败会恢复旧容器并保留失败退出码\n'
: >"$test_dir/commands.log"
migration_attempted=1
new_created=1
printf 'PGPASSWORD=sensitive\n' >"$test_dir/.pg-client.env"
if (trap cleanup EXIT; exit 43); then fail '失败状态未传递'; else [[ $? == 43 ]]; fi
[[ ! -e $test_dir/.pg-client.env ]]
! grep -q '^start ' "$test_dir/commands.log"
grep -q '^stop --time 60 platform$' "$test_dir/commands.log"
printf 'PASS: 迁移后失败停止新容器，不启动旧镜像，并清理临时密码文件\n'
rm -f -- "$test_dir/startup.log"
printf '全部 12 组行为检查通过。\n'
