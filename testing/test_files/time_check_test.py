from datetime import datetime
previous_minute = datetime.now().minute
print('Startup Minute: %s' % previous_minute)
while True:
    current_time = datetime.now()
    if current_time.minute != previous_minute:
        previous_minute = current_time.minute
        while True:
            current_time = datetime.now()
            if current_time.second == 5:
                print("Time to record!")
                break
