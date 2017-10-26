import Queue
import json
import random
import simulator

class Requesthandler:
    def __init__(self):
        with open("properties.json") as fp:
            config = json.load(fp)

        self.q = Queue.Queue()
        # current completion time of a queue
        self.completion_time = 0
        # TODO : Use this code of we want to use multiple queues
        #
        # self.write_server = config["server"]["writeServer"]
        # self.read_server = config["server"]["readServer"]
        #
        # self.list_queues = []
        # for i in range(0, config["server"]["numberOfServers"]):
        #     self.list_queues.append(Queue.Queue())



    def add_request(self, request):
        self.q.put(request)
        self.completion_time = self.completion_time + request["request_size"]
        event = {
            "time": self.completion_time + request["request_size"],
            "request": request,
            "callback": callback
        }

        simulator.schedule(event)


def callback(event):

    total_time_for_request = event["time"] - event["request"]["start_time"]
    print total_time_for_request





