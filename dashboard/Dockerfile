ARG NODE_VERSION=20.18.3
ARG ORCEST_BUILD_REVISION=unknown
FROM node:${NODE_VERSION}-slim AS builder
ENV NPM_CONFIG_ENGINE_STRICT=true \
    NPM_CONFIG_AUDIT=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_PROGRESS=false \
    NPM_CONFIG_UPDATE_NOTIFIER=false
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:${NODE_VERSION}-slim
ARG ORCEST_BUILD_REVISION=unknown
LABEL org.opencontainers.image.revision=${ORCEST_BUILD_REVISION}
ENV NPM_CONFIG_ENGINE_STRICT=true \
    NPM_CONFIG_AUDIT=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_PROGRESS=false \
    NPM_CONFIG_UPDATE_NOTIFIER=false
WORKDIR /app
RUN printf '%s\n' "${ORCEST_BUILD_REVISION}" > /app/.orcest-revision
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/build ./build
COPY --from=builder /app/scripts/check-bundle-runtime.mjs ./scripts/check-bundle-runtime.mjs
COPY --from=builder /app/package*.json ./
RUN npm ci --omit=dev && npm cache clean --force
RUN groupadd --gid 1001 nodejs \
  && useradd --uid 1001 --gid nodejs --home-dir /home/nodeuser --create-home --shell /usr/sbin/nologin nodeuser
USER nodeuser:nodejs
EXPOSE 8080
CMD ["node", "build/server/index.js"]
