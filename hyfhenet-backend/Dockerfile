FROM python:3.11.9-bullseye

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

COPY ./install-deps.sh /code/install-deps.sh

COPY ./.env /code/.env

RUN chmod +x /code/install-deps.sh

COPY ./app /code/app

COPY ./models /code/models

RUN python -m venv .venv

ENV PATH="/code/.venv/bin:$PATH"

RUN ./install-deps.sh

EXPOSE 80

CMD ["fastapi", "run", "app/main.py", "--port", "80", "--proxy-headers"]
