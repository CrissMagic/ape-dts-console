ARG NODE_IMAGE=node:24.19.0-bookworm-slim

FROM ${NODE_IMAGE} AS toolchain

ENV PNPM_HOME=/pnpm
ENV PATH=${PNPM_HOME}:${PATH}

RUN corepack enable && corepack prepare pnpm@10.12.4 --activate

WORKDIR /workspace

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY apps/api/package.json apps/api/package.json
COPY apps/web/package.json apps/web/package.json
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/config-model/package.json packages/config-model/package.json
COPY packages/database/package.json packages/database/package.json
COPY packages/security/package.json packages/security/package.json
COPY packages/storage/package.json packages/storage/package.json
COPY packages/observability/package.json packages/observability/package.json
COPY tools/artifact-probe/package.json tools/artifact-probe/package.json

RUN pnpm install --frozen-lockfile

FROM toolchain AS build

COPY . .

RUN pnpm build
RUN pnpm --filter @scs/api deploy --prod /opt/api

FROM ${NODE_IMAGE} AS runtime

ARG APP_GID=10001
ARG APP_UID=10001

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates tini xz-utils \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${APP_GID}" platform \
    && useradd --uid "${APP_UID}" --gid platform --no-create-home --shell /usr/sbin/nologin platform

WORKDIR /app

COPY --from=build --chown=${APP_UID}:${APP_GID} /opt/api/ /app/
COPY --from=build --chown=${APP_UID}:${APP_GID} /workspace/apps/web/dist/ /app/public/
COPY --chown=${APP_UID}:${APP_GID} docker/entrypoint/entrypoint.sh /app/docker/entrypoint/entrypoint.sh
COPY --chown=${APP_UID}:${APP_GID} LICENSE NOTICE THIRD_PARTY_NOTICES.md /app/licenses/

RUN install --directory --owner="${APP_UID}" --group="${APP_GID}" --mode=0700 \
      /run/ape-dts \
      /var/cache/ape-dts-platform/artifacts \
      /data/storage \
    && chmod 0555 /app/docker/entrypoint/entrypoint.sh

ENV HOST=0.0.0.0
ENV NODE_ENV=production
ENV PORT=3000
ENV WEB_DIST_PATH=/app/public

# Task subprocesses share the workload's CPU and memory limits set by Docker/Rancher.

EXPOSE 3000

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["node", "-e", "fetch('http://127.0.0.1:3000/api/health/live').then((response) => { if (!response.ok) process.exit(1) }).catch(() => process.exit(1))"]

USER ${APP_UID}:${APP_GID}

ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker/entrypoint/entrypoint.sh"]
