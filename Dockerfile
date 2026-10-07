FROM node:20-alpine
LABEL authors="ibroichenko"

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

USER node
EXPOSE 3000

CMD ["node", "src/main/kafkaService.js"]