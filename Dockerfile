# --- Base stage: Node LTS with pnpm ---
FROM node:22-slim AS base
ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

# --- Dependencies stage: install production deps only ---
FROM base AS deps
WORKDIR /app

# Copy workspace root manifests
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Copy package.json for each workspace package so pnpm can resolve the graph
COPY services/stream/package.json ./services/stream/
COPY lib/telemetry/package.json ./lib/telemetry/

RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile --prod

# --- Runtime stage ---
FROM base
WORKDIR /app

# Copy installed node_modules from deps stage
COPY --from=deps /app/ ./

# Copy application source
COPY lib/telemetry/ ./lib/telemetry/
COPY services/stream/ ./services/stream/

WORKDIR /app/services/stream

CMD ["node", "main.ts"]

