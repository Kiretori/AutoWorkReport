FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY . /app

ENV PREFECT_API_URL=http://prefect-server:4200/api

CMD ["sh", "-c", "sleep 5 && python main.py"]