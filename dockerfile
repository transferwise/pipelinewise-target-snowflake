FROM python:3.7
COPY . /app
WORKDIR /app
RUN   python3 -m pip install .[test]
ENTRYPOINT ["docker-entrypoint.sh"]