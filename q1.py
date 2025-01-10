import random
import heapq

# Events
ARRIVAL = 1
DEPARTURE = 2

class MM1Queue:
    def __init__(self, max_size, service_rate, event_list):
        self.queue = []
        self.max_size = max_size
        self.service_rate = service_rate
        self.event_list = event_list
        self.busy = False
        self.total_wait_time = 0.0
        self.num_customers_served = 0
        self.num_customers_dropped = 0

    def _append(self, item):
        if len(self.queue) < self.max_size:
            self.queue.append(item)
        else: # needed?
            self.num_customers_dropped += 1

    def _pop(self, index=0):
        return self.queue.pop(index) if self.queue else None
    
    def process_event(self, event_time, event_type):
        current_time = event_time
        

        if event_type == ARRIVAL:
            # print (f'arrival time: {current_time}')
            if not self.busy:
                self.busy = True
                service_time = random.expovariate(self.service_rate)
                heapq.heappush(self.event_list, (current_time + service_time, DEPARTURE))
                self._append(current_time)
            else:
                self._append(current_time)


        elif event_type == DEPARTURE:
            # print (f'departure time: {current_time}')
            if len(self.queue) > 0:
                self.num_customers_served += 1
                arrival_time = self._pop(0)
                wait_time = current_time - arrival_time
                self.total_wait_time += wait_time

                if len(self.queue) == 0:
                    self.busy = False
                else:
                    # Calculate service time and schedule the next departure
                    service_time = random.expovariate(self.service_rate)
                    heapq.heappush(self.event_list, (current_time + service_time, DEPARTURE))
            else:
                print("Error: Queue is empty")
                self.busy = False


    def __len__(self):
        return len(self.queue)

def simulation(arrival_rate=1.0, service_rate=2.0, simulation_time=5.0, max_queue_size=1000, seed=None):

    # Parameters
    arrival_rate = 1.0  # lambda
    service_rate = 2.0  # mu
    simulation_time = 50000  # Total simulation time
    max_queue_size = 1000  # Maximum queue size
    
    if (seed):
        random.seed(seed)

    # State variables
    current_time = 0.0
    event_list = []

    # Statistics
    num_in_queue = 0
    total_wait_time = 0.0
    num_customers_served = 0
    num_customers_dropped = 0 # needed?

    # Initialize the queue
    queue = MM1Queue(max_queue_size, service_rate, event_list)

    # set the first event
    event_time, event_type = (random.expovariate(arrival_rate), ARRIVAL)
    current_time = event_time

    while current_time < simulation_time:

        if event_type == ARRIVAL:
            next_arrival = current_time + random.expovariate(arrival_rate)
            heapq.heappush(event_list, (next_arrival, ARRIVAL))


        queue.process_event(event_time, event_type)

        #get next event
        event_time, event_type = heapq.heappop(event_list)
        current_time = event_time
        

    # Results
    average_wait_time = queue.total_wait_time / queue.num_customers_served if queue.num_customers_served > 0 else 0

    print(f"Number of customers served: {queue.num_customers_served}")
    print(f"Number of customers dropped: {queue.num_customers_dropped}")
    print(f"Average wait time: {average_wait_time}")

    avg_served = arrival_rate*simulation_time
    t_bar = 1/(service_rate - arrival_rate)
    print (f'avg total served error: {100*(abs(avg_served - queue.num_customers_served)/avg_served)} ')
    print(f'avg wait time error: {100*(abs(t_bar - average_wait_time)/t_bar)} ')



if __name__ == "__main__":
    simulation()