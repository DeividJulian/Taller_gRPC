# ---------- Etapa 1: compilar el frontend ----------
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# El código generado (gen/) ya viene en el repo; regenerarlo es opcional:
# RUN npx buf generate
RUN npm run build

# ---------- Etapa 2: imagen de producción ----------
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist
COPY gen ./gen
COPY server ./server

# Las plataformas cloud inyectan PORT; 8080 es el valor por defecto
ENV PORT=8080
EXPOSE 8080
USER node
CMD ["node_modules/.bin/tsx", "server/index.ts"]
