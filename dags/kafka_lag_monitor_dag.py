"""
Monitor Kafka consumer lag for Spark stream.
Schedule: */5 * * * *
"""
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaAdminClient, KafkaConsumer

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def check_kafka_lag():
    """Check consumer group lag and log warning if > 1000."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "crypto-trades"
    group_id = "spark-streaming-consumer"
    
    log.info(f"Connecting to Kafka AdminClient at {bootstrap_servers}")
    
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        
        group_offsets = admin.list_consumer_group_offsets(group_id)
        
        total_lag = 0
        lag_per_partition = {}
        
        for tp, offset_meta in group_offsets.items():
            if tp.topic == topic:
                end_offsets = consumer.end_offsets([tp])
                end_offset = end_offsets.get(tp, 0)
                
                lag = end_offset - offset_meta.offset
                total_lag += lag
                
                if lag > 0:
                    lag_per_partition[tp.partition] = lag
                    
        if total_lag > 1000:
            log.warning(f"HIGH KAFKA LAG DETECTED: Total lag={total_lag}. Partitions behind: {lag_per_partition}")
        else:
            log.info(f"Kafka lag is normal: {total_lag}")
            
    except Exception:
        log.exception("Error checking Kafka lag")
        raise

with DAG(
    'kafka_lag_monitor_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['monitoring'],
) as dag:

    check_lag_task = PythonOperator(
        task_id='check_kafka_lag',
        python_callable=check_kafka_lag,
    )
