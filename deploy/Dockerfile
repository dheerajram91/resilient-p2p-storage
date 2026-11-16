# Node agent image for the peer-container distributed storage system.
# Multi-stage: build TypeScript, then run a slim production image.

FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
RUN npm run build && npm prune --omit=dev

FROM node:24-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
# node:sqlite is still flagged as experimental; silence the per-start warning.
ENV NODE_NO_WARNINGS=1
ENV DATA_DIR=/data
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./

# libp2p TCP (P2P) and future client API (WebDAV / web UI / MCP)
EXPOSE 4001/tcp 8080/tcp

# Content, metadata and snapshot volumes.
VOLUME ["/data/content", "/data/meta", "/data/snapshots", "/data/identity"]

CMD ["node", "dist/index.js"]
