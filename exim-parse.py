#!/usr/bin/env python3
import re
import json
import os
import base64
import hashlib
import logging
from datetime import datetime
import argparse
from datetime import timezone
import glob
import shutil
import tempfile
import requests
from requests.auth import HTTPBasicAuth
import time
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlparse
import sys

# Suppress insecure HTTPS warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='exim_processor.log'
)
logger = logging.getLogger('exim_processor')

def decode_base64_subject(encoded_subject):
    """
    Special function to handle complex base64 encoded email subjects including truncated ones
    """
    # If no encoding markers are present, return as is
    if '=?utf-8?B?' not in encoded_subject:
        return encoded_subject
    
    try:
        # Split the subject by the base64 pattern
        pattern = r'(=\?utf-8\?B\?[^\?]+\?=?)'  # Note the optional closing '?='
        parts = re.split(pattern, encoded_subject)
        
        result = ""
        for part in parts:
            # Check if this part is base64 encoded
            match = re.match(r'=\?utf-8\?B\?([^\?]+)(\?=)?', part)
            if match:
                try:
                    # Extract the base64 part
                    base64_str = match.group(1)
                    
                    # Add padding if needed for proper base64 decoding
                    padding_needed = len(base64_str) % 4
                    if padding_needed:
                        base64_str += '=' * (4 - padding_needed)
                    
                    # Try to decode
                    try:
                        decoded = base64.b64decode(base64_str).decode('utf-8')
                        result += decoded
                    except Exception as e:
                        # If we fail, try with different approaches (like removing chars from the end)
                        for i in range(1, 4):
                            try:
                                # Try decoding with removing the last few characters
                                trimmed = base64_str[:-i] + '=' * i
                                decoded = base64.b64decode(trimmed).decode('utf-8')
                                result += decoded + "..." # Add ellipsis to indicate truncation
                                break
                            except:
                                continue
                        else:
                            # If all attempts fail, keep the original
                            logger.warning(f"Failed to decode truncated base64: {base64_str}")
                            result += f"[Encoded: {base64_str[:10]}...]"
                except Exception as e:
                    logger.warning(f"Failed to decode base64 part: {part}, error: {e}")
                    result += part  # Keep original if decoding fails
            else:
                # Not encoded, keep as is
                result += part
                
        return result
    except Exception as e:
        logger.error(f"Error decoding subject: {e}")
        return encoded_subject  # Return original if anything goes wrong

class ElasticsearchShipper:
    """Class to handle shipping data to Elasticsearch"""
    
    def __init__(self, es_url, index_name, username=None, password=None, verify_ssl=True):
        self.es_url = es_url.rstrip('/')
        self.index_name = index_name
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.bulk_data = []
        self.success_count = 0
        self.error_count = 0
        
        # Check if Elasticsearch is reachable
        self.check_connection()
        
        # Create index and mapping if needed
        self.create_index_if_not_exists()
    
    def check_connection(self):
        """Check if Elasticsearch is reachable"""
        try:
            auth = None
            if self.username and self.password:
                auth = HTTPBasicAuth(self.username, self.password)
            
            response = requests.get(
                f"{self.es_url}",
                auth=auth,
                verify=self.verify_ssl,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Elasticsearch connection failed: {response.status_code} - {response.text}")
                raise ConnectionError(f"Could not connect to Elasticsearch: {response.status_code}")
            
            logger.info(f"Successfully connected to Elasticsearch at {self.es_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            raise
    
    def create_index_if_not_exists(self):
        """Create the index with appropriate mappings if it doesn't exist"""
        auth = None
        if self.username and self.password:
            auth = HTTPBasicAuth(self.username, self.password)
        
        # Check if index exists
        response = requests.head(
            f"{self.es_url}/{self.index_name}",
            auth=auth,
            verify=self.verify_ssl
        )
        
        # If index doesn't exist (404), create it
        if response.status_code == 404:
            logger.info(f"Index {self.index_name} does not exist. Creating...")
            
            # Define the index mappings
            mappings = {
                "mappings": {
                    "properties": {
                        "exim_id": {"type": "keyword"},
                        "from_email": {"type": "keyword"},
                        "to_email": {"type": "keyword"},
                        "subject": {"type": "text"},
                        "@timestamp_received": {"type": "date"},
                        "@timestamp_delivered": {"type": "date"},
                        "@timestamp_completed": {"type": "date"},
                        "queue_time_seconds": {"type": "float"},
                        "status": {"type": "keyword"},
                        "source_host": {"type": "keyword"},
                        "smtp_host": {"type": "keyword"},
                        "delivery_code": {"type": "text"},
                        "completed": {"type": "boolean"},
                        "size_kb": {"type": "float"},
                        "dkim": {"type": "keyword"},
                        "host": {"type": "keyword"},
                        "bounce_reason": {"type": "keyword"},
                        "bounce_type": {"type": "keyword"},
                        "bounce_message": {"type": "text"},
                        "Router": {"type": "keyword"},
                        "Transport": {"type": "keyword"}
                    }
                }
            }
            
            # Create the index with mappings
            create_response = requests.put(
                f"{self.es_url}/{self.index_name}",
                json=mappings,
                auth=auth,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/json"}
            )
            
            if create_response.status_code not in (200, 201):
                logger.error(f"Failed to create index: {create_response.status_code} - {create_response.text}")
                raise Exception(f"Could not create index {self.index_name}")
            
            logger.info(f"Successfully created index {self.index_name}")
        else:
            logger.info(f"Index {self.index_name} already exists")
    
    def add_document(self, document):
        """Add a document to the bulk data queue"""
        # Format for bulk indexing
        action = {"index": {"_index": self.index_name}}
        
        # Add to bulk data array (action + document)
        self.bulk_data.append(json.dumps(action))
        self.bulk_data.append(json.dumps(document))
        
        # Ship in batches of 100 documents
        if len(self.bulk_data) >= 200:  # 100 documents = 200 lines
            self.ship_bulk_data()
    
    def ship_bulk_data(self):
        """Ship the queued documents to Elasticsearch in bulk"""
        if not self.bulk_data:
            return
        
        auth = None
        if self.username and self.password:
            auth = HTTPBasicAuth(self.username, self.password)
        
        # Prepare bulk request body
        bulk_body = "\n".join(self.bulk_data) + "\n"
        
        try:
            response = requests.post(
                f"{self.es_url}/_bulk",
                data=bulk_body,
                auth=auth,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/x-ndjson"}
            )
            
            if response.status_code not in (200, 201):
                logger.error(f"Bulk import failed: {response.status_code} - {response.text}")
                self.error_count += len(self.bulk_data) // 2
            else:
                result = response.json()
                if result.get('errors', False):
                    # Count errors
                    error_count = sum(1 for item in result.get('items', []) if item.get('index', {}).get('status', 200) >= 400)
                    success_count = (len(self.bulk_data) // 2) - error_count
                    
                    logger.warning(f"Bulk import completed with {error_count} errors out of {len(self.bulk_data) // 2} documents")
                    self.error_count += error_count
                    self.success_count += success_count
                else:
                    logger.info(f"Successfully indexed {len(self.bulk_data) // 2} documents")
                    self.success_count += len(self.bulk_data) // 2
        except Exception as e:
            logger.error(f"Error sending bulk data to Elasticsearch: {str(e)}")
            self.error_count += len(self.bulk_data) // 2
        
        # Clear bulk data after shipping
        self.bulk_data = []
    
    def ship_from_file(self, json_file_path):
        """Read a JSON file (one document per line) and ship to Elasticsearch"""
        doc_count = 0
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    try:
                        document = json.loads(line.strip())
                        self.add_document(document)
                        doc_count += 1
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON in file: {line}")
                        self.error_count += 1
        except Exception as e:
            logger.error(f"Error reading file {json_file_path}: {str(e)}")
            return False
        
        # Ship any remaining documents
        if self.bulk_data:
            self.ship_bulk_data()
        
        logger.info(f"Completed shipping from file. Processed {doc_count} documents. "
                   f"Success: {self.success_count}, Errors: {self.error_count}")
        
        return True
    
    def get_stats(self):
        """Get shipping statistics"""
        return {
            "success_count": self.success_count,
            "error_count": self.error_count,
            "total_processed": self.success_count + self.error_count
        }


class EximLogProcessor:
    def __init__(self, log_file_path, output_file='exim_logs.json', temp_file='exim_temp.json', 
                 es_ship=False, es_url=None, es_index=None, es_user=None, es_pass=None, es_verify_ssl=True):
        self.log_file_path = log_file_path
        # Add timestamp to output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename, ext = os.path.splitext(output_file)
        self.final_output_file = f"{filename}_{timestamp}{ext}"
        
        # Create temp output file path
        self.temp_output_file = os.path.join(tempfile.gettempdir(), os.path.basename(self.final_output_file))
        self.temp_file = temp_file
        self.messages = {}
        self.completed_messages = []
        
        # Elasticsearch shipping options
        self.es_ship = es_ship
        self.es_shipper = None
        
        if es_ship and es_url and es_index:
            try:
                self.es_shipper = ElasticsearchShipper(
                    es_url=es_url,
                    index_name=es_index,
                    username=es_user,
                    password=es_pass,
                    verify_ssl=es_verify_ssl
                )
                logger.info("Elasticsearch shipper initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Elasticsearch shipper: {str(e)}")
                self.es_ship = False
        
        # Delete previous timestamped files
        self.cleanup_old_files(filename, ext)
        
        # Load any existing incomplete messages
        self.load_incomplete_messages()
    
    def cleanup_old_files(self, filename, ext):
        """Delete previous timestamped files"""
        pattern = f"{filename}_*{ext}"
        old_files = glob.glob(pattern)
        for old_file in old_files:
            try:
                os.remove(old_file)
                logger.info(f"Deleted previous output file: {old_file}")
            except Exception as e:
                logger.error(f"Failed to delete {old_file}: {e}")
    
    def load_incomplete_messages(self):
        """Load incomplete messages from temporary file if it exists"""
        if os.path.exists(self.temp_file):
            try:
                with open(self.temp_file, 'r', encoding='utf-8') as file:
                    try:
                        temp_data = json.load(file)
                        if isinstance(temp_data, dict):
                            self.messages = temp_data
                            logger.info(f"Loaded {len(self.messages)} incomplete messages from {self.temp_file}")
                    except json.JSONDecodeError:
                        logger.warning(f"Temp file {self.temp_file} contains invalid JSON. Starting with empty cache.")
            except (IOError, UnicodeDecodeError) as e:
                logger.error(f"Error reading temp file: {e}")
    
    def save_incomplete_messages(self):
        """Save incomplete messages to temporary file"""
        try:
            # Remove empty fields before saving
            clean_messages = {}
            for msg_id, msg_data in self.messages.items():
                clean_messages[msg_id] = {k: v for k, v in msg_data.items() if v or isinstance(v, (bool, int, float))}
            
            with open(self.temp_file, 'w', encoding='utf-8') as file:
                json.dump(clean_messages, file, ensure_ascii=False, indent=2)
                logger.info(f"Saved {len(clean_messages)} incomplete messages to {self.temp_file}")
        except (IOError, UnicodeEncodeError) as e:
            logger.error(f"Error writing to temp file: {e}")
    
    def save_completed_messages(self):
        """Save completed messages to temp output file in JSON format and/or ship to Elasticsearch"""
        try:
            if not self.completed_messages:
                return
                
            # Write to file if not shipping or shipping but still want local file
            mode = 'a' if os.path.exists(self.temp_output_file) else 'w'
            with open(self.temp_output_file, mode, encoding='utf-8') as file:
                for message in self.completed_messages:
                    # Remove empty fields before saving
                    clean_message = {k: v for k, v in message.items() if v or isinstance(v, (bool, int, float))}
                    
                    # If shipping to Elasticsearch, send each document
                    if self.es_ship and self.es_shipper:
                        self.es_shipper.add_document(clean_message)
                    
                    # Always write to the file
                    file.write(json.dumps(clean_message, ensure_ascii=False) + '\n')
                
                logger.info(f"Saved {len(self.completed_messages)} completed messages to temp file {self.temp_output_file}")
                self.completed_messages = []  # Clear the list after saving
                
        except (IOError, UnicodeEncodeError) as e:
            logger.error(f"Error writing to temp output file: {e}")
    
    def finalize_output(self):
        """Move temp output file to final destination and finish shipping to Elasticsearch"""
        try:
            # If shipping to Elasticsearch, make sure any remaining documents are sent
            if self.es_ship and self.es_shipper and self.es_shipper.bulk_data:
                self.es_shipper.ship_bulk_data()
                stats = self.es_shipper.get_stats()
                logger.info(f"Elasticsearch shipping complete: {stats['success_count']} successes, {stats['error_count']} errors")
            
            if os.path.exists(self.temp_output_file):
                # Create output directory if it doesn't exist
                os.makedirs(os.path.dirname(os.path.abspath(self.final_output_file)), exist_ok=True)
                shutil.move(self.temp_output_file, self.final_output_file)
                logger.info(f"Moved temp output file to final destination: {self.final_output_file}")
                return True
            return True  # Even without a file, we've completed processing
        except Exception as e:
            logger.error(f"Error moving temp output file to final destination: {e}")
            return False
    
    def parse_log_line(self, line):
        """Parse a single log line and extract relevant information"""
        # Basic log line pattern
        pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\S+) (.+)'
        match = re.match(pattern, line)
        
        if not match:
            return None
        
        timestamp, message_id, content = match.groups()
        
        # Initialize message record if it's a new message
        if message_id not in self.messages:
            self.messages[message_id] = {
                "exim_id": message_id,
                "from_email": "",
                "to_email": "",
                "subject": "",
                "@timestamp_received": "",
                "@timestamp_delivered": "",
                "@timestamp_completed": "",
                "queue_time_seconds": 0,
                "status": "",
                "source_host": "",
                "smtp_host": "",
                "delivery_code": "",
                "completed": False,
                "size_kb": 0,
                "dkim": "",
                "host": "",
                "bounce_reason": "",
                "bounce_type": "",
                "bounce_message": "",
                "Router": "",
                "Transport": ""
            }
        
        # Process incoming message
        if '<=' in content:
            self.process_incoming(timestamp, message_id, content)
            # Check if subject is "Mail delivery failed: returning message to sender"
            if self.messages[message_id]["subject"] == "Mail delivery failed: returning message to sender":
                del self.messages[message_id]  # Remove the message from processing
                return None
        # Process completed message
        elif 'Completed' in content:
            self.process_completed(timestamp, message_id, content)
        # Process successful dispatch
        elif '=>' in content or '->' in content:
            self.process_dispatch_successful(timestamp, message_id, content)
        # Process retry timeout exceeded
        elif 'retry timeout exceeded' in content:
            self.process_retry_timeout(timestamp, message_id, content)
        # Process hard bounce (only if NOT retry timeout)
        elif '**' in content and 'retry timeout exceeded' not in content:
            self.process_hard_bounce(timestamp, message_id, content)
        # Process soft bounce
        elif '==' in content:
            self.process_soft_bounce(timestamp, message_id, content)
        
        return message_id
    
    def process_incoming(self, timestamp, message_id, content):
        """Process incoming message log line"""
        # Update timestamp received
        self.messages[message_id]["@timestamp_received"] = timestamp
        
        # Extract from email
        from_match = re.search(r'<=\s+(\S+)', content)
        if from_match:
            self.messages[message_id]["from_email"] = from_match.group(1)
        
        # Extract source host
        host_match = re.search(r'H=([^\s\(]+)(?:\s+\(([^\)]+)\))?', content)
        if host_match:
            self.messages[message_id]["source_host"] = host_match.group(1)
            if host_match.group(2):
                self.messages[message_id]["host"] = host_match.group(2)
        
        # Extract size in bytes and convert to kilobytes
        size_match = re.search(r'S=(\d+)', content)
        if size_match:
            size_bytes = int(size_match.group(1))
            # Convert bytes to kilobytes (rounded to 2 decimal places)
            size_kb = round(size_bytes / 1024, 2)
            self.messages[message_id]["size_kb"] = size_kb
        
        # Extract DKIM
        dkim_match = re.search(r'DKIM=(\S+)', content)
        if dkim_match:
            self.messages[message_id]["dkim"] = f"dkim={dkim_match.group(1)}"
        
        # Extract subject
        subject_match = re.search(r'T="([^"]+)"', content)
        if subject_match:
            encoded_subject = subject_match.group(1)
            # Use dedicated function to properly handle base64 encoded subjects
            decoded_subject = decode_base64_subject(encoded_subject)
            self.messages[message_id]["subject"] = decoded_subject
    
    def process_dispatch_successful(self, timestamp, message_id, content):
        """Process successful dispatch log line"""
        # Extract to email and hash it
        to_match = re.search(r'=>\s+(\S+)', content)
        if not to_match:
            to_match = re.search(r'->\s+(\S+)', content)
        if to_match:
            to_email = to_match.group(1)
            # MD5 hash the to address directly in the to_email field
            self.messages[message_id]["to_email"] = hashlib.md5(to_email.encode()).hexdigest()
        
        # Update timestamp delivered
        self.messages[message_id]["@timestamp_delivered"] = timestamp

        # Format @timestamp_delivered
        delivered_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        self.messages[message_id]["@timestamp_delivered"] = delivered_time.replace(tzinfo=timezone.utc).isoformat()
        
        # Extract smtp host
        host_match = re.search(r'H=([^\s\[]+)(?:\s+\[([^\]]+)\])?', content)
        if host_match:
            self.messages[message_id]["smtp_host"] = host_match.group(1)
        
        # Extract Router and Transport
        router_match = re.search(r'R=(\S+)', content)
        if router_match:
            self.messages[message_id]["Router"] = router_match.group(1)
        
        transport_match = re.search(r'T=(\S+)', content)
        if transport_match:
            self.messages[message_id]["Transport"] = transport_match.group(1)
        
        # Set status to successful
        self.messages[message_id]["status"] = "delivered"
        
        # Extract delivery code if available
        code_match = re.search(r'C="([^"]+)"', content)
        if code_match:
            self.messages[message_id]["delivery_code"] = code_match.group(1)
    
    def process_hard_bounce(self, timestamp, message_id, content):
        """Process hard bounce log line"""
        # Ensure this isn't a retry timeout
        if 'retry timeout exceeded' in content:
            logger.warning(f"Retry timeout incorrectly classified as hard bounce for {message_id}")
            return  # Skip processing as hard bounce

        # Extract to email and hash it
        to_match = re.search(r'\*\*\s+(\S+)', content)
        if to_match:
            to_email = to_match.group(1).rstrip(':')
            # MD5 hash the to address directly in the to_email field
            self.messages[message_id]["to_email"] = hashlib.md5(to_email.encode()).hexdigest()

        # Update status
        self.messages[message_id]["status"] = "hard_bounce"
        self.messages[message_id]["bounce_type"] = "hard"
        
        # Extract Router and Transport
        router_match = re.search(r'R=(\S+)', content)
        if router_match:
            self.messages[message_id]["Router"] = router_match.group(1)
        
        transport_match = re.search(r'T=(\S+)', content)
        if transport_match:
            self.messages[message_id]["Transport"] = transport_match.group(1)
        
        # Extract bounce reason
        error_match = re.search(r'SMTP error.*?: (.*?)$', content)
        if error_match:
            self.messages[message_id]["bounce_message"] = error_match.group(1).replace('\n', ' ')
            reason_match = re.search(r'(\d{3}[- ]\d+\.\d+\.\d+)', self.messages[message_id]["bounce_message"])
            if reason_match:
                self.messages[message_id]["bounce_reason"] = reason_match.group(1)
        
        # Extract smtp host
        host_match = re.search(r'H=([^\s\[]+)(?:\s+\[([^\]]+)\])?', content)
        if host_match:
            self.messages[message_id]["smtp_host"] = host_match.group(1)
    
    def process_soft_bounce(self, timestamp, message_id, content):
        """Process soft bounce log line"""
        # Extract to email and hash it
        to_match = re.search(r'==\s+(\S+)', content)
        if to_match:
            to_email = to_match.group(1).rstrip(':')
            # MD5 hash the to address directly in the to_email field
            self.messages[message_id]["to_email"] = hashlib.md5(to_email.encode()).hexdigest()
        
        # Update status (will be updated to soft_bounce_final if retry timeout exceeded is found)
        self.messages[message_id]["status"] = "soft_bounce"
        self.messages[message_id]["bounce_type"] = "soft"
        
        # Extract Router and Transport
        router_match = re.search(r'R=(\S+)', content)
        if router_match:
            self.messages[message_id]["Router"] = router_match.group(1)
        
        transport_match = re.search(r'T=(\S+)', content)
        if transport_match:
            self.messages[message_id]["Transport"] = transport_match.group(1)
        
        # Extract bounce reason
        error_match = re.search(r'SMTP error.*?: (.*?)$', content)
        if error_match:
            self.messages[message_id]["bounce_message"] = error_match.group(1).replace('\n', ' ')
            reason_match = re.search(r'(\d{3}[- ]\d+\.\d+\.\d+)', self.messages[message_id]["bounce_message"])
            if reason_match:
                self.messages[message_id]["bounce_reason"] = reason_match.group(1)
        
        # Extract smtp host
        host_match = re.search(r'H=([^\s\[]+)(?:\s+\[([^\]]+)\])?', content)
        if host_match:
            self.messages[message_id]["smtp_host"] = host_match.group(1)

    def process_soft_bounce(self, timestamp, message_id, content):
        """Process soft bounce log line"""
        # Extract to email and hash it
        to_match = re.search(r'==\s+(\S+)', content)
        if to_match:
            to_email = to_match.group(1).rstrip(':')
            self.messages[message_id]["to_email"] = hashlib.md5(to_email.encode()).hexdigest()

        # Set initial bounce type and status
        self.messages[message_id]["status"] = "soft_bounce"
        self.messages[message_id]["bounce_type"] = "soft"

        # Extract Router and Transport
        router_match = re.search(r'R=(\S+)', content)
        if router_match:
            self.messages[message_id]["Router"] = router_match.group(1)

        transport_match = re.search(r'T=(\S+)', content)
        if transport_match:
            self.messages[message_id]["Transport"] = transport_match.group(1)

        # Extract bounce reason (e.g., "Connection refused")
        error_match = re.search(r'H=.*? \[.*?\] (.*)', content)
        if error_match:
            self.messages[message_id]["bounce_message"] = error_match.group(1)
            self.messages[message_id]["bounce_reason"] = error_match.group(1)

    def process_retry_timeout(self, timestamp, message_id, content):
        """Process retry timeout exceeded log line"""
        # Transition soft bounce to soft_bounce_final
        if self.messages[message_id]["bounce_type"] == "soft":
            self.messages[message_id]["status"] = "soft_bounce_final"

        # Extract retry timeout reason if available
        reason_match = re.search(r'retry timeout exceeded.*?: (.*)', content)
        if reason_match:
            self.messages[message_id]["bounce_message"] = reason_match.group(1)
            self.messages[message_id]["bounce_reason"] = reason_match.group(1)

        # If no specific reason is found, inherit from prior soft bounce processing
        if not self.messages[message_id]["bounce_reason"]:
            # Fallback to find the last known error message
            self.messages[message_id]["bounce_message"] = self.messages[message_id].get("bounce_message", "Unknown reason")
            self.messages[message_id]["bounce_reason"] = self.messages[message_id].get("bounce_reason", "Unknown reason")

    
    def process_completed(self, timestamp, message_id, content):
        """Process completed message log line"""
        # Update timestamp completed and calculate queue time
        self.messages[message_id]["@timestamp_completed"] = timestamp
        self.messages[message_id]["completed"] = True
        
        # Format @timestamp_completed
        completed_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        self.messages[message_id]["@timestamp_completed"] = completed_time.replace(tzinfo=timezone.utc).isoformat()

        # Calculate queue time
        try:
            if self.messages[message_id]["@timestamp_received"]:
                # If @timestamp_received is already in ISO format, handle it differently
                if isinstance(self.messages[message_id]["@timestamp_received"], str):
                    if 'T' in self.messages[message_id]["@timestamp_received"]:
                        # Already in ISO format, use datetime.fromisoformat
                        received_time = datetime.fromisoformat(self.messages[message_id]["@timestamp_received"].replace('Z', '+00:00'))
                    else:
                        # Old string format
                        received_time = datetime.strptime(self.messages[message_id]["@timestamp_received"], "%Y-%m-%d %H:%M:%S")
                        self.messages[message_id]["@timestamp_received"] = received_time.replace(tzinfo=timezone.utc).isoformat()
                
                completed_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                queue_time = (completed_time - received_time).total_seconds()
                self.messages[message_id]["queue_time_seconds"] = queue_time
        except (ValueError, TypeError) as e:
            logger.warning(f"Error calculating queue time for {message_id}: {e}")

        # If message is completed, check if it should be included in output
        should_output = False
        
        # For delivered messages, always output
        if self.messages[message_id]["status"] == "delivered":
            should_output = True
        # For hard bounce messages, always output
        elif self.messages[message_id]["status"] == "hard_bounce":
            should_output = True
        # For soft bounce messages, only output if retry timeout exceeded
        elif self.messages[message_id]["status"] == "soft_bounce_final":
            should_output = True
        
        if should_output:
            # Add to completed messages for output
            self.completed_messages.append(self.messages[message_id])
            # Remove from active messages
            del self.messages[message_id]
        else:
            # If this is a soft bounce that's completed but not "final", we can discard it
            if self.messages[message_id]["status"] == "soft_bounce":
                del self.messages[message_id]
    
    def process_log_file(self):
        """Process the entire log file"""
        try:
            with open(self.log_file_path, 'r', encoding='utf-8', errors='replace') as file:
                for line in file:
                    try:
                        self.parse_log_line(line.strip())
                    except Exception as e:
                        logger.error(f"Error processing line: {line.strip()}: {e}")
            
            # After processing, save completed and incomplete messages
            if self.completed_messages:
                self.save_completed_messages()
            
            self.save_incomplete_messages()
            
            # Move temp output file to final destination
            success = self.finalize_output()
            
            return success
        except (IOError, UnicodeDecodeError) as e:
            logger.error(f"Error reading log file {self.log_file_path}: {e}")
            return False


# This is the main entry point - OUTSIDE the class definition
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Exim logs into JSON format for Elasticsearch')
    parser.add_argument('--log-file', required=True, help='Path to the Exim log file')
    parser.add_argument('--output-file', default='exim_logs.json', help='Path to the output JSON file')
    parser.add_argument('--temp-file', default='exim_temp.json', help='Path to the temporary file for incomplete messages')
    
    # Add Elasticsearch options
    parser.add_argument('--es-ship', action='store_true', help='Ship logs directly to Elasticsearch')
    parser.add_argument('--es-url', help='Elasticsearch URL (e.g., http://localhost:9200)')
    parser.add_argument('--es-index', default='exim-logs', help='Elasticsearch index name')
    parser.add_argument('--es-user', help='Elasticsearch username')
    parser.add_argument('--es-pass', help='Elasticsearch password')
    parser.add_argument('--es-no-verify', action='store_true', help='Disable SSL certificate verification')
    
    # Add option to ship existing file to Elasticsearch
    parser.add_argument('--ship-existing', help='Ship an existing JSON file to Elasticsearch without processing new logs')
    
    args = parser.parse_args()
    
    # Validate Elasticsearch arguments
    if args.es_ship and not args.es_url:
        print("Error: --es-url is required when --es-ship is specified")
        sys.exit(1)
    
    # Ship existing file if requested
    if args.ship_existing and args.es_ship and args.es_url:
        try:
            es_shipper = ElasticsearchShipper(
                es_url=args.es_url,
                index_name=args.es_index,
                username=args.es_user,
                password=args.es_pass,
                verify_ssl=not args.es_no_verify
            )
            
            if os.path.exists(args.ship_existing):
                print(f"Shipping existing file {args.ship_existing} to Elasticsearch...")
                success = es_shipper.ship_from_file(args.ship_existing)
                
                if success:
                    stats = es_shipper.get_stats()
                    print(f"Successfully shipped to Elasticsearch. Processed: {stats['total_processed']}, "
                          f"Success: {stats['success_count']}, Errors: {stats['error_count']}")
                else:
                    print("Failed to ship to Elasticsearch. Check logs for details.")
            else:
                print(f"Error: File {args.ship_existing} does not exist")
                sys.exit(1)
        except Exception as e:
            print(f"Error shipping to Elasticsearch: {str(e)}")
            sys.exit(1)
        sys.exit(0)
    
    # Process log file
    processor = EximLogProcessor(
        log_file_path=args.log_file,
        output_file=args.output_file,
        temp_file=args.temp_file,
        es_ship=args.es_ship,
        es_url=args.es_url,
        es_index=args.es_index,
        es_user=args.es_user,
        es_pass=args.es_pass,
        es_verify_ssl=not args.es_no_verify
    )
    
    success = processor.process_log_file()
    
    if success:
        print(f"Successfully processed log file. Output saved to {processor.final_output_file}")
        
        # Print Elasticsearch stats if applicable
        if args.es_ship and processor.es_shipper:
            stats = processor.es_shipper.get_stats()
            print(f"Elasticsearch shipping results: Processed: {stats['total_processed']}, "
                  f"Success: {stats['success_count']}, Errors: {stats['error_count']}")
    else:
        print("Error processing log file. Check the logs for details.")
    
