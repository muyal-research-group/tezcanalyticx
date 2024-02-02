FROM python:3.8
WORKDIR /app
COPY ./requirements.txt .
RUN pip install -r /app/requirements.txt
COPY ./src .
CMD ["python","/app/main.py"]