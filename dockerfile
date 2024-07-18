FROM python:3.9
COPY . /src
# COPY ./rsa_key.p8 /rsa_key.p8
WORKDIR /src
RUN python3 -m pip install .[test]
RUN mkdir /app
WORKDIR /app
ENTRYPOINT ["bash"]