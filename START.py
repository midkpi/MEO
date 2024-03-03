import subprocess
import time
from datetime import datetime

log_file = "restart_logs.txt"

while True:
    try:
        with open(log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Бот запущен\n")
        subprocess.run(['python', 'main3.py'], check=True)
    except subprocess.CalledProcessError:
        print('main3.py crashed, restarting...')
        with open(log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Бот сломался...\n")
        continue
    else:
        break
