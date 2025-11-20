FROM python:3.13-slim
# syntax=docker/dockerfile:1
# needed by unstructured
#RUN apt-get update && apt-get install build-essential -y && apt-get install -y libglib2.0-0 libsm6 libxrender1 libxext6 libgl1 && apt-get install -y poppler-utils && apt-get -y install python3-pil tesseract-ocr libtesseract-dev tesseract-ocr-eng tesseract-ocr-script-latn && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /code
COPY requirements.txt .
# RUN pip3 install -r requirements.txt
# Use following command to in local development to keep python packages in cache
RUN --mount=type=cache,target=/root/.cache pip3 install -r requirements.txt
COPY . .
EXPOSE 8000
ENTRYPOINT ["gunicorn", "--timeout", "0", "-b", ":8000", "app:app", "--log-level", "debug", "--capture-output"]