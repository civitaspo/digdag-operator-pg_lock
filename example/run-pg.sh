#!/usr/bin/env bash

exec docker run \
    -it \
    --rm \
    -p 5432:5432 \
    -e POSTGRES_DB=digdag \
    -e POSTGRES_USER=digdag \
    -e POSTGRES_PASSWORD=digdag \
    postgres:10

