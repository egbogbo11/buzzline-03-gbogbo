"""
json_producer_gbogbo.py

Stream JSON data to a Kafka topic.

Example JSON message
{"message": "I love Python!", "author": "Eve"}

Example serialized to Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"
"""

#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import json
from typing import Generator, Dict, Any


from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "json_gbogbo")   
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("buzz.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path) -> Generator[Dict[str, Any], None, None]:
    """
    Read from a JSON file and yield them one by one, continuously.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as json_file:
                logger.info(f"Reading data from file: {file_path}")

                json_data: list[Dict[str, Any]] = json.load(json_file)
                for buzz_entry in json_data:
                    if buzz_entry.get("author") == "Alice":
                        logger.warning(f"Special author detected: {buzz_entry}")
                    logger.debug(f"Generated JSON: {buzz_entry}")
                    yield buzz_entry

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.
    """
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages(DATA_FILE):
            producer.send(topic, value=message_dict)
            # --- Minimal enhancement: info + alert on keyword in message
            text = str(message_dict.get("message", ""))
            if "python" in text.lower():
                logger.warning(f"ALERT keyword 'python' in message: {text}")
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close(timeout=None)
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
