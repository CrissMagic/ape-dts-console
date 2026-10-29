# Ape DTS Console

[中文](README.md) | English

A self-hosted data synchronization management platform powered by [ape-dts](https://github.com/apecloud/ape-dts). Configure pipelines, manage replication resources, inspect progress and logs, and handle recovery and alerts through a web interface.

This is an independent community project, not an official ApeCloud product. The platform and engine run as separate programs. Engine artifacts are uploaded by operators and are not bundled in this source tree or the default image.

## Supported pipelines

| Pipeline           | Transfer mode                             | Platform support                                                                          |
| ------------------ | ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| PostgreSQL → Kafka | Optional initial snapshot followed by CDC | Table/topic mappings, replication slots and publications, preflight, monitoring, recovery |
| MySQL → Kafka      | Optional initial snapshot followed by CDC | Binlog inspection, mappings, checkpoints, recovery, explicit reinitialization             |
| Kafka → Kafka Raw  | Continuous forwarding (extended engine)   | Multi-topic mappings, partition execution units, consumer-group progress                  |

**Continuous Kafka-to-Kafka raw forwarding is an extension in the community-maintained fork, not a standard capability of the current upstream ape-dts release.** It requires an engine artifact containing that extension. An upstream binary cannot be assumed to support every task shown by the console. See [engine provenance and compatibility](docs/operations/engine-compatibility.md).

Other features include task cards and shared groups, server-side datasource search and pagination, datasource access grants, artifact uploads, live filtered logs, historical runs, metrics retained for up to seven days, WeCom alerts, configurable automatic recovery, audit records, and configuration import/export without passwords. The UI is currently Chinese, supports light/dark themes, and displays dates in Asia/Shanghai.

Doris can currently be registered and inspected as a datasource but cannot be selected as a task sink. See the [roadmap](docs/roadmap.md) for planned pipelines. Actual compatibility depends on the engine artifact, database versions, and deployment environment.

## Docker quick start

Requirements: Linux x86_64, Docker, an external PostgreSQL database dedicated to platform metadata, and reachable source/sink services. Tasks run as child processes in the platform container and share its resource limits. Docker Socket, privileged mode, and cgroup v2 delegation are not required.

```sh
git clone https://github.com/CrissMagic/ape-dts-console.git
cd ape-dts-console
docker build -t ape-dts-console:local .
sudo install -d -o 10001 -g 10001 -m 0700 \
  /srv/ape-dts-console/storage /srv/ape-dts-console/artifact-cache
install -m 0600 .env.example platform.env
# Edit platform.env: configure a dedicated metadata database and a unique password.

docker run -d --name ape-dts-console \
  --restart unless-stopped --stop-timeout 30 \
  --cpus 4 --memory 8g \
  --log-driver json-file --log-opt max-size=10m --log-opt max-file=3 \
  --env-file ./platform.env \
  --env SCS_PLATFORM_VERSION="$(git rev-parse --short HEAD)" \
  -p 127.0.0.1:18088:3000 \
  --mount type=bind,src=/srv/ape-dts-console/storage,dst=/data/storage \
  --mount type=bind,src=/srv/ape-dts-console/artifact-cache,dst=/var/cache/ape-dts-platform/artifacts \
  ape-dts-console:local
```

Visit `http://127.0.0.1:18088/`. After successful initialization, container logs show a random temporary password for `admin`. Change it at first login and restrict access to these logs. Normal restarts do not reset the password.

Resource limits above are examples, not a capacity guarantee. The current HTTP session policy is intended for trusted private networks; native HTTPS support is not implemented. For remote access, bind a controlled private address and restrict network access.

**Back up the metadata database and storage directory together.** Storage contains the key needed to decrypt credentials. Only one active platform controller may use a given metadata database.

Detailed documentation is primarily Chinese: [deployment](docs/operations/docker-server-deployment.md), [updates and recovery](docs/operations/server-update-script.md), [administrator recovery](docs/operations/password-authentication.md). Legacy `scs-data-sync-platform` identifiers, environment variables, database schema, and container paths remain compatible.

## Development

Use Node.js 24 and pnpm 10.12.4 with a dedicated development database.

```sh
corepack enable
corepack pnpm install --frozen-lockfile
corepack pnpm build:packages
corepack pnpm dev
```

Inject the variables documented in `.env.example` into the development process; the file is not loaded automatically. API defaults to port 3000; Vite prints the frontend URL. See the [development guide](docs/development.md) for local paths and integration-test requirements.

```sh
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

## Community

[Documentation](docs/README.md) · [Roadmap](docs/roadmap.md) · [Changelog](CHANGELOG.md) · [Issues](https://github.com/CrissMagic/ape-dts-console/issues) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Code of conduct](CODE_OF_CONDUCT.md)

Chinese is the default language; English contributions are welcome. Do not include credentials or unredacted operational data in issues or pull requests.

## License

Original platform code is licensed under [Apache-2.0](LICENSE). Retained third-party code keeps its own license; see [NOTICE](NOTICE) and [third-party notices](THIRD_PARTY_NOTICES.md). The separately distributed ape-dts engine is AGPL-3.0. The platform license does not relicense the engine or remove source-availability obligations for distributed engine builds or modifications.
