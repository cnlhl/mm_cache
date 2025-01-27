# 提供方法： 1. check 2. get

from flask import Flask, request, jsonify
import logging
from cache_auto import CacheAuto

app = Flask(__name__)
data_cache = CacheAuto(config_file='config.json')

# 设置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/request/<data_id>', methods=['GET'])
def handle_request(data_id):
    logging.debug('REQUEST received')
    shape = data_cache.get(data_id)  # 假设这返回的是一个 NumPy 数组的形状
    if shape is None:
        return "NOT_FOUND", 404
    return jsonify({'shape': list(shape)}), 200

@app.route('/check', methods=['GET'])
def look_cached():
    logging.debug('CHECK received')
    cached = data_cache.check()
    return jsonify(cached)

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=6000)
