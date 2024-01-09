import time
from datetime import datetime, timedelta

from importer import main

####################################
while True:
    now = datetime.utcnow()
    if now.minute < 15:
        delay = 15-now.minute
        if delay < 0:
            delay = 60+delay
        print(f"waiting for {delay} minutes")
        time.sleep(60*delay)
        continue
    else:
        subed = now+timedelta(hours=-3)
        start = datetime(subed.year, subed.month, subed.day, subed.hour)
        print(f"start importing data from {start}")
        main.delay(start.isoformat())
        print(f"importing data from {start} finished, sleepingg for 1 hour!")
        time.sleep(3600)
