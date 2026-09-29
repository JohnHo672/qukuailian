# The base is tagged locally from the already verified official image. This
# deployment stays buildable when Docker Hub is unavailable; startup also
# verifies that the embedded Hummingbot Core is exactly 20260920.
FROM local/hummingbot-api-base:20260920

WORKDIR /hummingbot-api

# Overlay the reviewed API source on the pinned runtime.
COPY main.py version.py config.py deps.py ./
COPY models ./models
COPY routers ./routers
COPY services ./services
COPY utils ./utils
COPY database ./database
COPY bots/controllers ./bots/controllers
COPY bots/scripts ./bots/scripts

RUN mkdir -p bots/instances bots/conf bots/credentials bots/data bots/archived

EXPOSE 8000
ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
