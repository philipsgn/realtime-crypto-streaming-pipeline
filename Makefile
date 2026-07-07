.PHONY: up down producer spark-job logs clean

up:
	docker compose -f infrastructure/docker-compose.yml up -d --build
	@echo "Services started. Grafana: http://localhost:3000"

down:
	docker compose -f infrastructure/docker-compose.yml down

producer:
	python ingestion/binance_producer.py

spark-job:
	python processing/spark_streaming.py

logs:
	docker compose -f infrastructure/docker-compose.yml logs -f

clean:
	docker compose -f infrastructure/docker-compose.yml down
	@echo "Stopped containers. Named volumes are preserved."
	@echo "Do not use docker compose down -v unless you intentionally want to delete data."
