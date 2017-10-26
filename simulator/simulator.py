import Queue


# XXX: Override the comparator of prioority queue to support our functionality

Q = Queue.PriorityQueue()
time = 0

# {"a":12312132312, "b":123123, "c":123}

def schedule(event):
        Q.put((event["time"], event))


def run():
    global time
    while not Q.empty():
        time, event = Q.get()
        event["callback"](event)

    return


def reset():
    global time
    time = 0
    while not Q.empty():
        temp = Q.get()

    return