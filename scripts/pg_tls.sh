#!/usr/bin/env bash
# PostgreSQL с TLS для теста tests/db_tls.rs: свой УЦ, серверный сертификат на localhost, контейнер на 127.0.0.1:5434.
#
#   scripts/pg_tls.sh [каталог]          # сертификаты — в каталоге (по умолчанию ./target/pg-tls)
#   IT_DATABASE_TLS_URL=jdbc:postgresql://localhost:5434/scada_tls IT_DATABASE_TLS_CA=<каталог>/ca.crt \
#     cargo test --test db_tls -- --ignored
#   docker rm -f scada-pg-tls            # убрать
set -euo pipefail

dir="$(mkdir -p "${1:-target/pg-tls}" && cd "${1:-target/pg-tls}" && pwd)"
cd "$dir"
rm -f ca.* server.* ext.cnf

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -keyout ca.key -out ca.crt -subj "/CN=Test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=localhost" 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' > ext.cnf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -out server.crt -extfile ext.cnf 2>/dev/null
chmod 644 server.key

docker rm -f scada-pg-tls >/dev/null 2>&1 || true
# Ключ должен принадлежать postgres и иметь права 600 — копируем внутри контейнера.
docker run -d --name scada-pg-tls -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=scada_tls \
  -p 127.0.0.1:5434:5432 -v "$dir":/certs:ro postgres:16 bash -c \
  'cp /certs/server.* /var/lib/postgresql/ && chown postgres:postgres /var/lib/postgresql/server.* \
   && chmod 600 /var/lib/postgresql/server.key && exec docker-entrypoint.sh postgres -c ssl=on \
   -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key' >/dev/null

for _ in $(seq 60); do
  docker exec scada-pg-tls pg_isready -U postgres -d scada_tls >/dev/null 2>&1 && exit 0
  sleep 1
done
echo "PostgreSQL с TLS не поднялся" >&2
docker logs scada-pg-tls >&2 || true
exit 1
