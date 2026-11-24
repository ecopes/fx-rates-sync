FROM python:3.13-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip install --no-cache-dir build && python -m build --wheel --outdir /wheels

FROM python:3.13-slim
RUN useradd --create-home --uid 10001 fx
COPY --from=build /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
USER fx
WORKDIR /data
ENV FX_DB_PATH=/data/fx_rates.db
VOLUME /data
ENTRYPOINT ["fx-sync"]
CMD ["sync"]
