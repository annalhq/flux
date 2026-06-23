# Build context is the repo root so the image installs from the shared uv.lock.
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONPATH=/app

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups --group producers

COPY producers/schemas ./schemas
COPY producers/flux_producers ./flux_producers

RUN useradd --uid 10001 --no-create-home flux
USER flux
EXPOSE 8000
ENTRYPOINT ["python", "-m", "flux_producers"]
