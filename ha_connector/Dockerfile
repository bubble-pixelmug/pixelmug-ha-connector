FROM ghcr.io/home-assistant/base:latest

ARG BUILD_VERSION="dev"
ARG BUILD_ARCH="unknown"

LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="${BUILD_ARCH}"

RUN apk add --no-cache nodejs npm

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --ignore-scripts --no-audit --no-fund && npm cache clean --force

COPY src ./src
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
