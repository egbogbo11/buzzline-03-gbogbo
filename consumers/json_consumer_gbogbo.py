"""
json_consumer_gbogbo.py

Consume json messages from a Kafka topic and process them.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences
from typing import Any  # <-- moved here

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
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
    """Fetch Kafka topic from environment or use default."""
    # tiny uniqueness tweak: make the default your custom topic
    topic = os.getenv("BUZZ_TOPIC", "json_gbogbo")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Minimal extra config (env-driven)
#####################################

# Comma-separated authors to watch, e.g., "Alice,Eve"
WATCH_AUTHORS = {a.strip() for a in os.getenv("WATCH_AUTHORS", "Alice,Eve").split(",") if a.strip()}

# Comma-separated keywords to flag, e.g., "distributed,stream,python"
WATCH_KEYWORDS = {k.strip().lower() for k in os.getenv("WATCH_KEYWORDS", "distributed,stream,python").split(",") if k.strip()}

# Summarize every N messages
try:
    SUMMARY_EVERY = int(os.getenv("SUMMARY_EVERY", "20"))
except ValueError:
    SUMMARY_EVERY = 20

#####################################
# Set up Data Store to hold author counts
#####################################

# Initialize a dictionary to store author counts
author_counts: defaultdict[str, int] = defaultdict(int)

# Minimal global counter for periodic summaries
messages_seen: int = 0

#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    global messages_seen
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict[str, Any] = json.loads(message)  # <-- uses Any from top import

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Extract the 'author' field from the Python dictionary
        author = str(message_dict.get("author", "unknown"))
        text = str(message_dict.get("message", ""))

        logger.info(f"Message received from author: {author}")

        # Increment the count for the author
        author_counts[author] += 1
        messages_seen += 1

        # --- Minimal custom analytics via logs (unique enhancement) ---
        # 1) Warn on watched authors
        if author in WATCH_AUTHORS:
            logger.warning(f"ALERT author watch -> {author}")

        # 2) Warn on keyword hits
        tl = text.lower()
        hits = [kw for kw in WATCH_KEYWORDS if kw in tl]
        if hits:
            logger.warning(f"ALERT keyword(s) {hits} in message: {text}")

        # 3) Periodic summary every N messages
        if SUMMARY_EVERY > 0 and messages_seen % SUMMARY_EVERY == 0:
            # top 3 authors (no extra imports)
            top3 = sorted(author_counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
            logger.info(f"[SUMMARY] messages={messages_seen} top_authors={top3}")

        # Log the updated counts
        logger.debug(f"Updated author counts: {dict(author_counts)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            # poll returns a dict: {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    # value_deserializer in utils_consumer already decoded this to str
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        
    logger.info(f"Kafka consumer for topic '{topic}' closed.")
    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")
    

#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
