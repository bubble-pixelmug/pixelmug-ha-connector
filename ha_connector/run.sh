#!/usr/bin/with-contenv bashio

bashio::log.info "Starting PixelMug HA Connector"
exec node /app/src/index.js
