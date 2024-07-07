# 
FROM python:3.9

# 
WORKDIR /app

# 
COPY ./requirements.txt /app/requirements.txt

# 
RUN pip install -r /app/requirements.txt
#
COPY ./pyproject.toml /app
COPY ./tezcanalyticx /app/tezcanalyticx
# ENV MICTLANX_ROUTER_PORT=60666
# ENV MICTLANX_ROUTER_HOST=0.0.0.0