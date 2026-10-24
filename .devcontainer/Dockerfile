FROM python:3.14-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends git postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash vscode \
    && python -m venv /opt/venv \
    && chown -R vscode:vscode /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /workspaces/meridian
COPY requirements.lock /tmp/requirements.lock
USER vscode
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock

CMD ["sleep", "infinity"]