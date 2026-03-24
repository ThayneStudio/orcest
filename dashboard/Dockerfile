FROM node:20.18.3-slim AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20.18.3-slim
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/build ./build
COPY --from=builder /app/package*.json ./
RUN npm ci --omit=dev
RUN addgroup --system --gid 1001 nodejs && adduser --system --uid 1001 nodeuser
USER nodeuser
EXPOSE 8080
CMD ["node", "build/server/index.js"]
