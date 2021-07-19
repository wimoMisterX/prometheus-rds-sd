FROM python:3.8-slim-buster
COPY *.py *.txt /
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "discovery.py"]
