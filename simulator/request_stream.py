from distribution import Distribution
from arrival import Arrival
from request import Request
from request_handler import Requesthandler


import simulator

class RequestStream():
    def __init__(self):
        self.arrival = Arrival()
        self.request = Request()

        self.add_arrival_event()


    def add_arrival_event(self):
        request = self.request.next_request()
        request["start_time"] = self.arrival.next_arrival()


        event = {
            "request" : self.request.next_request(),
            "time": request["start_time"],
            "callback" : callback
        }

        simulator.schedule(event)
        return


def callback(event):
    Requesthandler.add_request(event)
    return