"""
csv_consumer_gbogbo.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque

from dotenv import load_dotenv

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "csv_gbogbo")  # unique default
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation


def get_rolling_window_size() -> int:
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Detect a stall
#####################################


def detect_stall(rolling_window_deque: deque) -> bool:
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        logger.debug(
            f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}."
        )
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Process a single message
#####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the temperature reading
        if rolling_window and temperature < rolling_window[-1] - 10:
            logger.warning(
                f"ALERT Rapid temperature drop detected: {temperature}°F at {timestamp}"
            )
        rolling_window.append(temperature)

        if detect_stall(rolling_window):
            logger.info(
                f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings."
            )

        # --- minimal enhancement: summary when deque fills ---
        if len(rolling_window) == window_size:
            avg_temp = sum(rolling_window) / window_size
            logger.info(
                f"[SUMMARY] Window full ({window_size} readings). Avg temp={avg_temp:.1f}°F"
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main
#####################################


def main() -> None:
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            raw = message.value
            message_str = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
