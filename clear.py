import os

shm_directory = '/dev/shm'
prefix = 'shm'

for filename in os.listdir(shm_directory):
    if filename.startswith(prefix):
        file_path = os.path.join(shm_directory, filename)
        try:
            os.remove(file_path)
            print(f"Removed {file_path}")
        except Exception as e:
            print(f"Failed to remove {file_path}: {e}")