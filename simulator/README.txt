Simulator :

Inputs:
System specification and initialization - how many servers, their locations (which may affect response times in a more advanced version), their contents when the simulation begins
Arrival process - determines the arrival times of requests (either generated as per a specified distribution or in the form of a trace), may have an additional feature in a more advanced version denoting where a request originates from
In addition to arrival time, each request has a “type” (put/get/delete for KV and more complex for others) and an “origin” (in case we simulate distributed clients later on).

Routing and load balancing policies - the policies that determine for each request by whom it will be serviced
A routing policy may be needed if we are simulating geo-distributed servers and clients and requests should be sent to the “nearest” servers
A load balancing policy may be needed if there are multiple options for servicing

Scheduling policy at each server - default could be FIFO, more advanced could be priority-based, e.g., useful in case of cancellation of requests
Failure process - In a future version, failures may be simulated