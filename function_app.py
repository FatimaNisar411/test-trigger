

import azure.functions as func
import logging
import requests
import csv
import json
import openai
import io
from datetime import date, datetime
import os

app = func.FunctionApp()
#------------------------------------------------------   
#HTTP TRIGGER
#------------------------------------------------------   

@app.function_name(name="WeatherHttp")
@app.route(route="weather", auth_level=func.AuthLevel.ANONYMOUS)  # easier to test
def weather_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Weather HTTP trigger function processed a request.")

    # Step 1: Get city parameter
    city = req.params.get("city")
    if not city:
        try:
            req_body = req.get_json()
            city = req_body.get("city")
        except Exception:
            pass

    if not city:
        return func.HttpResponse(
            "Please pass a 'city' in the query string or body (e.g. ?city=Toronto)",
            status_code=400
        )

    # Step 2: Get coordinates using free geocoding API (Open-Meteo)
    geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1"
    geo_resp = requests.get(geo_url).json()

    if "results" not in geo_resp or len(geo_resp["results"]) == 0:
        return func.HttpResponse(
            f"Could not find coordinates for city: {city}",
            status_code=404
        )

    lat = geo_resp["results"][0]["latitude"]
    lon = geo_resp["results"][0]["longitude"]
    resolved_city = geo_resp["results"][0]["name"]

    # Step 3: Get weather
    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    weather_resp = requests.get(weather_url).json()
    temp = weather_resp["current_weather"]["temperature"]

    # Step 4: Build response
    result = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "city": resolved_city,
        "latitude": lat,
        "longitude": lon,
        "temperature_celsius": temp
    }

    return func.HttpResponse(
        body=str(result),
        status_code=200,
        mimetype="application/json"
    )
    
#------------------------------------------------------   
#TIMER TRIGGER
#------------------------------------------------------   

@app.function_name(name="WeatherTimer")
@app.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=True, use_monitor=True)
@app.blob_input(arg_name="inputBlob", path="weather/weather.csv", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputBlob", path="weather/weather.csv", connection="AzureWebJobsStorage")
def weather_timer(mytimer: func.TimerRequest, inputBlob: bytes, outputBlob: func.Out[str]):
    logging.info("Weather Timer trigger started.")

    # Example city: Toronto
    city = "Toronto"
    url = "https://api.open-meteo.com/v1/forecast?latitude=43.7&longitude=-79.4&hourly=temperature_2m"
    response = requests.get(url)
    data = response.json()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    temp = data["hourly"]["temperature_2m"][0]

    # === Step 1: Read existing CSV (if any) ===
    rows = []
    if inputBlob:
        existing_data = inputBlob
        reader = csv.reader(io.StringIO(existing_data))
        rows = list(reader)

    # === Step 2: If empty, add header ===
    if not rows:
        rows.append(["timestamp", "city", "temperature"])

    # === Step 3: Append new record ===
    rows.append([now, city, temp])

    # === Step 4: Write back to blob ===
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)

    outputBlob.set(output.getvalue())
    logging.info(f"Weather data appended: {rows[-1]}")
    
    
#------------------------------------------------------
# Blob Trigger → Save Weather CSV Row to Cosmos DB
#------------------------------------------------------
@app.function_name(name="SaveWeatherCsvToCosmos")
@app.blob_trigger(arg_name="inputBlob", path="weather/weather.csv", connection="AzureWebJobsStorage")
def save_weather_csv_to_cosmos(inputBlob: bytes):
    import csv, io
    from azure.cosmos import CosmosClient, PartitionKey
    cosmos_conn_str = os.getenv("COSMOS_CONNECTION_STRING")
    cosmos_db = os.getenv("WEATHER_COSMOS_DATABASE", "weatherdb")
    cosmos_container = os.getenv("WEATHER_COSMOS_CONTAINER", "weather")
    if not cosmos_conn_str:
        logging.warning("No COSMOS_CONNECTION_STRING found in environment; skipping Cosmos DB save.")
        return

    try:
        # Parse CSV
        reader = csv.DictReader(io.StringIO(inputBlob.decode("utf-8")))
        rows = list(reader)
        if not rows:
            logging.info("No weather data to save to Cosmos DB.")
            return
        # Save only the last row (newest entry)
        record = rows[-1]
        # Add an id for Cosmos DB
        record["id"] = record["timestamp"].replace(" ", "_") + "_" + record["city"]
        cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
        db = cosmos_client.create_database_if_not_exists(id=cosmos_db)
        container = db.create_container_if_not_exists(
            id=cosmos_container,
            partition_key=PartitionKey(path="/city")
        )
        container.upsert_item(record)
        logging.info(f"✅ Saved weather record to Cosmos DB: {record}")
    except Exception as e:
        logging.error(f"❌ Failed to save weather to Cosmos DB: {e}")    
    
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

#------------------------------------------------------
# HTTP → Queue (enqueue one question per request)
#------------------------------------------------------
@app.function_name(name="EnqueueQuestion")
@app.route(route="enqueue", auth_level=func.AuthLevel.ANONYMOUS)
@app.queue_output(arg_name="msg", queue_name="questionsqueue", connection="AzureWebJobsStorage")
def enqueue_question(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    logging.info("HTTP trigger received request to enqueue one question.")
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body.", status_code=400)

    question = body.get("question")
    if not question:
        return func.HttpResponse("Please provide a 'question'.", status_code=400)

    payload = {"question": question}
    logging.info(f"📬 Enqueuing: {payload}")
    msg.set(json.dumps(payload))

    return func.HttpResponse("✅ Question enqueued.", status_code=200)



#------------------------------------------------------
# Queue → Process with OpenAI → Append to Blob and Cosmos DB
#------------------------------------------------------
from azure.cosmos import CosmosClient, PartitionKey

@app.function_name(name="AnswerQuestions")
@app.queue_trigger(arg_name="msg", queue_name="questionsqueue", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputBlob", path="answers/answers.json", connection="AzureWebJobsStorage")
def answer_questions(msg: str, outputBlob: func.Out[str]) -> None:
    data = json.loads(msg)
    question = data["question"]

    logging.info(f"❓ Processing question: {question}")

    # ✅ Call OpenAI properly
    response = client.chat.completions.create(
        model="gpt-4o-mini",   # you can also use gpt-3.5-turbo
        messages=[{"role": "user", "content": question}],
        max_tokens=50
    )
    answer = response.choices[0].message.content.strip()
    logging.info(f"✅ Answer: {answer}")

    # Build record
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    record = {
        "id": f"{now.replace(' ', '_').replace(':', '-')}_{hash(question)}",  # Cosmos DB requires unique id
        "timestamp": now,
        "question": question,
        "answer": answer
    }


    # Robust append to blob using Azure Blob SDK (handles concurrency)
    from azure.storage.blob import BlobServiceClient
    blob_conn_str = os.getenv("AzureWebJobsStorage")
    blob_service = BlobServiceClient.from_connection_string(blob_conn_str)
    container_client = blob_service.get_container_client("answers")
    blob_name = "answers.json"
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Download existing data
            try:
                blob_client = container_client.get_blob_client(blob_name)
                download = blob_client.download_blob()
                existing = json.loads(download.readall() or b"[]")
            except Exception:
                existing = []

            # Append and upload
            existing.append(record)
            data = json.dumps(existing, indent=2)
            # Use ETag for concurrency control
            if 'blob_client' in locals() and blob_client.exists():
                etag = download.properties.etag
                blob_client.upload_blob(data, overwrite=True, if_match=etag)
            else:
                container_client.upload_blob(blob_name, data, overwrite=True)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"❌ Failed to append to blob after {max_retries} attempts: {e}")
            else:
                import time
                time.sleep(0.5)
                continue

    # Save to Cosmos DB
    cosmos_conn_str = os.getenv("COSMOS_CONNECTION_STRING")
    cosmos_db = os.getenv("COSMOS_DATABASE", "answersdb")
    cosmos_container = os.getenv("COSMOS_CONTAINER", "answers")
    if cosmos_conn_str:
        try:
            cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
            db = cosmos_client.create_database_if_not_exists(id=cosmos_db)
            container = db.create_container_if_not_exists(
                id=cosmos_container,
                partition_key=PartitionKey(path="/question")
            )
            container.upsert_item(record)
            logging.info("✅ Saved answer to Cosmos DB.")
        except Exception as e:
            logging.error(f"❌ Failed to save to Cosmos DB: {e}")
    else:
        logging.warning("No COSMOS_CONNECTION_STRING found in environment; skipping Cosmos DB save.")

#------------------------------------------------------
# Cosmos DB Trigger → Count Today's Questions
#------------------------------------------------------
@app.function_name(name="CosmosCountToday")
@app.cosmos_db_trigger(
    arg_name="items",
    database_name=os.getenv("COSMOS_DATABASE", "answersdb"),
    container_name=os.getenv("COSMOS_CONTAINER", "answers"),
    connection="COSMOS_CONNECTION_STRING",
    lease_container_name="leases",
    create_lease_container_if_not_exists=True
)
def cosmos_count_today(items: func.DocumentList):
    if not items:
        return
    today = date.today().strftime("%Y-%m-%d")
    cosmos_conn_str = os.getenv("COSMOS_CONNECTION_STRING")
    cosmos_db = os.getenv("COSMOS_DATABASE", "answersdb")
    cosmos_container = os.getenv("COSMOS_CONTAINER", "answers")
    try:
        from azure.cosmos import CosmosClient
        cosmos_client = CosmosClient.from_connection_string(cosmos_conn_str)
        db = cosmos_client.get_database_client(cosmos_db)
        container = db.get_container_client(cosmos_container)
        query = f"SELECT VALUE COUNT(1) FROM c WHERE STARTSWITH(c.timestamp, '{today}')"
        count = list(container.query_items(query=query, enable_cross_partition_query=True))[0]
        logging.info(f"📊 Number of questions asked on {today}: {count}")
    except Exception as e:
        logging.error(f"Failed to count today's questions in Cosmos DB: {e}")