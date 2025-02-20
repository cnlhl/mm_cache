# 提供方法： 1. check 2. get

from flask import Flask, request, jsonify
import logging
from cache_auto import CacheAuto
import os
import sys
import signal
import threading

app = Flask(__name__)
data_cache = None
shutdown_event = threading.Event()

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

def signal_handler(signum, frame):
    """处理终止信号"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()
    if data_cache:
        try:
            data_cache.stop()
        except:
            pass
    # 直接退出程序
    sys.exit(0)

def create_app():
    global data_cache
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        data_cache = CacheAuto(config_file='config.json')
    except Exception as e:
        logger.error(f"Failed to initialize cache: {e}")
        sys.exit(1)
    
    return app

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
    # 从请求参数中获取data_type
    data_type = request.args.get('data_type', None)
    logger.debug(f'Checking cache with data_type: {data_type}')
    cached = data_cache.check(data_type)
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

def run_flask_app():
    app = create_app()
    try:
        from werkzeug.serving import make_server
        server = make_server('localhost', 6000, app)
        
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True  # 将服务器线程设置为守护线程
        server_thread.start()

        # 等待关闭信号
        shutdown_event.wait()
        
        logger.info("Shutting down the server...")
        server.shutdown()
        
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        if data_cache:
            try:
                data_cache.stop()
            except:
                pass
        logger.info("Server stopped")

if __name__ == '__main__':
    run_flask_app()