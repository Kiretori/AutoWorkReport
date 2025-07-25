FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

ENV PREFECT_API_URL=http://prefect-server:4200/api

CMD ["python", "main.py"]
