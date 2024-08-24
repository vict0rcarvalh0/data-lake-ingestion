FROM python:3.12.4-slim

RUN pip install poetry

COPY . .

RUN poetry install --no-cache

CMD [ "poetry", "run", "python", "./app.py" ]