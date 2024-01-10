import time
from datetime import datetime, timedelta

from importer import auto_task_runner, main

####################################

# Check if missed file exists
open('missed', 'a').close()


auto_task_runner.delay(
    interval=60,
    conditions=[
        "len(get_running_tasks()) == 1",
        "15 < datetime.utcnow().minute < 50"
    ],
    _callable="cherry_pick"
)

while True:
    # TODO: Check if utc should be used
    now = datetime.utcnow()
    if now.minute < 15:
        delay = 15-now.minute
        # if delay < 0:
        #     delay = 60+delay
        print(f"waiting for {delay} minutes")
        time.sleep(60*delay)
    else:
        start = now+timedelta(
            hours=-3,
            minutes=-now.minute,
            seconds=-now.second,
            microseconds=-now.microsecond
        )
        print(f"start importing data from {start}")
        main.delay(start.isoformat())
        print(f"loop sleeping for 1 hour!")
        time.sleep(3600)
