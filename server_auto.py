# 提供方法： 1. check 2. get

from flask import Flask, request, jsonify
import logging
from cache_auto import CacheAuto
import os
import sys

app = Flask(__name__)
data_cache = CacheAuto(config_file='config.json')

log_directory = './log'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger('server_logger')
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('./log/server.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

@app.route('/request/<data_id>', methods=['GET'])
def handle_request(data_id):
    try:
        logger.debug(f'REQUEST received for data_id: {data_id}')
        shape = data_cache.get(data_id) 
        if shape is None:
            logger.warning(f'Data not found for id: {data_id}')
            return jsonify({'error': 'Data not found'}), 404
        return jsonify({'shape': list(shape)}), 200
    except Exception as e:
        logger.error(f'Error processing request for {data_id}: {e}')
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/check', methods=['GET'])
def look_cached():
    logger.debug('CHECK received')
    cached = data_cache.check()
    return jsonify(cached)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'cache_status': {
            'loaded_count': len(data_cache.loaded_queue),
            'unloaded_count': len(data_cache.unloaded_queue)
        }
    }), 200

if __name__ == '__main__':
    try:
        app.run(debug=False, host='localhost', port=6000)
    except KeyboardInterrupt:
        data_cache.stop()
        logger.info('Server stopped')