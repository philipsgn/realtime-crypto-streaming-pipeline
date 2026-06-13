.PHONY: up down producer spark-job logs clean

up:
	docker compose -f infrastructure/docker-compose.yml up -d
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
	docker compose -f infrastructure/docker-compose.yml down -v
	rm -rf /tmp/crypto_raw /tmp/checkpoint
