FROM python:3.8

# Install dependencies
#RUN apt install libffi-dev openssl-dev build-base python3-dev git
RUN apt-get update
RUN apt-get install -y build-essential

# Install python dependencies
RUN pip install -U pip
RUN pip install poetry
COPY ./poetry.lock ./pyproject.toml /app/
WORKDIR /app
RUN poetry config virtualenvs.create false --local
RUN poetry install --no-dev

# Copy the rest of the project, so the above is cached if requirements didn't change
COPY ./ /app/

CMD ["python3", "-u", "src/main.py", "--start-api"]
