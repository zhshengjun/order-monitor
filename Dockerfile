FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOST=0.0.0.0 PORT=8080 DATABASE_PATH=/data/monitor.db
WORKDIR /app
RUN groupadd --gid 10001 monitor && useradd --uid 10001 --gid 10001 --create-home monitor && mkdir /data && chown monitor:monitor /data
COPY --chown=monitor:monitor app.py /app/app.py
COPY --chown=monitor:monitor static /app/static
COPY docker-entrypoint.py /app/docker-entrypoint.py
ENTRYPOINT ["python", "/app/docker-entrypoint.py"]
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2)"
CMD ["python", "app.py"]
