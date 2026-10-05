FROM node:22-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
# Node 22 strips TypeScript types at load time, so we run the .ts source directly.
CMD ["node", "src/server.ts"]
