FROM mcr.microsoft.com/mssql/server:2022-latest

COPY --chmod=444 certs/server.* /certs/
COPY --chmod=444 certs/customCA.* /certs/
USER root
RUN chown -R 10001:10001 /certs && chmod 755 /certs && chmod 644 /certs/*
USER 10001
COPY --chown=10001:10001 docker-mssql.conf /var/opt/mssql/mssql.conf
