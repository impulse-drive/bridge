FROM alpine:latest
RUN apk --no-cache add nodejs yarn
COPY . /app/
WORKDIR /app/
RUN yarn install
ENTRYPOINT ["yarn", "start"]


