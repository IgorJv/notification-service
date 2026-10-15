FROM node:20-alpine
LABEL authors="ibroichenko"

WORKDIR /app

COPY --chown=node:node package*.json ./
RUN npm install --production

COPY --chown=node:node . .

USER node
EXPOSE 8080

CMD ["node", "src/main/kafkaService.js"]