FROM python:3.7
COPY . /src
WORKDIR /src
RUN python3 -m pip install .[test]
RUN mkdir /app
WORKDIR /app
ENTRYPOINT ["bash"]