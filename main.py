from flask import Flask, request, jsonify
import redis
import pymysql
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import traceback
import sys
import os

app = Flask(__name__)

# Configure Redis
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = 6379
redis_db = 0
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

# Configure MariaDB
db_host = os.environ.get('DB_HOST', 'localhost')
db_user = 'root'
db_password = ''
db_name = 'mmta'
db_connection = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name, cursorclass=pymysql.cursors.DictCursor)

# Configure Elasticsearch
es_host = os.environ.get('ES_HOST', 'localhost')
es_port = 9200
es_scheme = 'https'
es_index = 'log_index'
es_username = 'elastic'  # Replace with your Elasticsearch username
es_password = '1TcWhIycsGHmN5qtea7g'  # Replace with your Elasticsearch password

# Disable SSL certificate verification (only for development, not recommended in production)
es_client = Elasticsearch(
    [{'host': es_host, 'port': es_port, 'scheme': es_scheme}],
    verify_certs=False,
    http_auth=(es_username, es_password)
)

@app.route('/sync', methods=['GET', 'POST', 'DELETE'])
def sync_data():
    try:
        if request.method == 'GET':
            # Get data from the GET request
            data_id = request.args.get('id')

            if data_id is None:
                # If no specific ID is provided, retrieve all records from MariaDB
                with db_connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM your_table")
                    result = cursor.fetchall()
                    return jsonify(result), 200
            else:
                # If a specific ID is provided, check if data exists in MariaDB
                with db_connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM your_table WHERE id = %s", (data_id,))
                    result = cursor.fetchone()
                    if result:
                        return jsonify(result), 200
                    else:
                        return jsonify({'message': 'Data not found in MariaDB'}), 404

        elif request.method == 'POST':
            # Get data from the POST request
            data = request.get_json()

            # Check if data exists in Redis
            if not redis_client.exists(data['id']):
                # If data doesn't exist in Redis, perform a SET request to Redis
                redis_client.set(data['id'], data['value'])
                # Check MariaDB for the timestamp
                with db_connection.cursor() as cursor:
                    cursor.execute("SELECT timestamp FROM your_table WHERE id = %s", (data['id'],))
                    result = cursor.fetchone()
                    if result:
                        timestamp = result['timestamp']
                        # Check if timestamp is older than 10 mins
                        if datetime.utcnow() - timestamp > timedelta(minutes=10):
                            # Perform an update in MariaDB
                            with db_connection.cursor() as update_cursor:
                                update_cursor.execute("UPDATE your_table SET timestamp = NOW() WHERE id = %s", (data['id'],))
                                db_connection.commit()
                            # Write a log to Elasticsearch
                            log_entry = {
                                'timestamp': datetime.utcnow(),
                                'operation': 'Update in MariaDB',
                                'data_id': data['id']
                            }
                            es_client.index(index=es_index, body=log_entry)
                    else:
                        # If data doesn't exist, perform an INSERT operation in MariaDB
                        with db_connection.cursor() as insert_cursor:
                            insert_cursor.execute("INSERT INTO your_table (id, value, timestamp) VALUES (%s, %s, NOW())",
                                                (data['id'], data['value']))
                            db_connection.commit()

                        # Write a log to Elasticsearch
                        log_entry = {
                            'timestamp': datetime.utcnow(),
                            'operation': 'Insert in MariaDB',
                            'data_id': data['id']
                        }
                        es_client.index(index=es_index, body=log_entry)
            else:
                # If data exists in Redis, return without further processing
                return jsonify({'message': 'Data already exists in Redis'}), 200

            # Write a log to Elasticsearch
            log_entry = {
                'timestamp': datetime.utcnow(),
                'operation': 'SET in Redis',
                'data_id': data['id']
            }
            es_client.index(index=es_index, body=log_entry)

            return jsonify({'message': 'Sync successful'}), 200

        elif request.method == 'DELETE':
            # Get data from the DELETE request
            data = request.get_json()

            # Check if data exists in Redis, and delete if it does
            if redis_client.exists(data['id']):
                redis_client.delete(data['id'])
                # Write a log to Elasticsearch
                log_entry = {
                    'timestamp': datetime.utcnow(),
                    'operation': 'DELETE in Redis',
                    'data_id': data['id']
                }
                es_client.index(index=es_index, body=log_entry)
                return jsonify({'message': 'Data deleted from Redis'}), 200
            else:
                return jsonify({'message': 'Data not found in Redis'}), 404

    except Exception as e:
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

@app.route('/bootstrap')
def bootstrap():
    try:
        # Check database connection
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            db_connection.commit()

        # Check if the table exists, create if it doesn't
        with db_connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS your_table (
                    id INT PRIMARY KEY,
                    value VARCHAR(255) NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            db_connection.commit()

        # Check Redis connection
        redis_client.ping()

        # Check Elasticsearch connection
        es_client.info()

        return jsonify({'message': 'Bootstrap successful'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    health_status = {
        'database': check_database(),
        'redis': check_redis(),
        'elasticsearch': check_elasticsearch()
    }

    return jsonify(health_status), 200

def check_database():
    try:
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            db_connection.commit()
        return {'status': 'ok'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def check_redis():
    try:
        redis_client.ping()
        return {'status': 'ok'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def check_elasticsearch():
    try:
        es_client.info()
        return {'status': 'ok'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
