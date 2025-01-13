import random
import heapq
import sys

# Events
ARRIVAL = 1
DEPARTURE = 2


class Event:
    def __init__(self, event_time, event_type, to = None):
        self.time = event_time
        self.type = event_type
        self.to = to
    
    def __lt__(self, other):
        return self.time < other.time
    
class Task: #TODO: should assert task.id == event.task_id?
    def __init__(self, arrival_time):
        self.arrival_time = arrival_time
        self.before_service = None
        self.after_service = None

def get_next_time(current_time, rate):
    return current_time + random.expovariate(rate)


class MM1Queue:
    def __init__(self, id,  max_size, service_rate, event_list):
        self.id = id
        self.queue = []
        self.max_size = max_size
        self.service_rate = service_rate
        self.event_list = event_list
        self.busy = False
        self.total_wait_time = 0.0
        self.total_service_time = 0.0
        self.num_customers_served = 0
        self.num_customers_dropped = 0
        self.t_end = 0.0

    def _append(self, item):
        if len(self.queue) < self.max_size:
            self.queue.append(item)
        else: # needed?
            self.num_customers_dropped += 1

    def _pop(self, index=0):
        return self.queue.pop(index) if self.queue else None
    
    def process_event(self, event):
        event_time = event.time
        event_type = event.type
        current_time = event_time
        

        if event_type == ARRIVAL:
            task = Task(arrival_time= current_time)
            if not self.busy:
                self.busy = True

                #handle immediately when empty
                after_service_time = get_next_time(current_time, self.service_rate)
                event = Event(after_service_time, DEPARTURE, to=self.id)
                heapq.heappush(self.event_list, event)

                task.before_service = current_time
                self._append(task)
            else:
                self._append(task)


        elif event_type == DEPARTURE:
            if len(self.queue) > 0:
                # got finished task to clean up
                self.num_customers_served += 1

                task = self._pop(0)
                task.after_service = current_time

                wait_time = task.before_service - task.arrival_time
                service_time = task.after_service - task.before_service

                self.total_wait_time += wait_time
                self.total_service_time += service_time
                self.t_end = current_time   

                if len(self.queue) == 0:
                    self.busy = False
                else:
                    # handle next task
                    after_service_time = get_next_time(current_time, self.service_rate) 
                    self.queue[0].before_service = current_time

                    event = Event(after_service_time, DEPARTURE, to=self.id)
                    heapq.heappush(self.event_list, event)
            else:
                print("Error: Queue is empty")
                self.busy = False


    def __len__(self):
        return len(self.queue)

class LoadBalancer:
    def __init__(self, num_servers, p_list, queue_sizes, service_rates, event_list):
        self.num_servers = num_servers
        self.p_list = p_list
        self.queue_sizes = queue_sizes
        self.service_rates = service_rates
        self.event_list = event_list
        self.queues = [MM1Queue(i, queue_sizes[i] + 1, service_rates[i], event_list) for i in range(num_servers)]

    def _select_queue(self):
        return random.choices(self.queues, weights=self.p_list, k=1)[0]
    
    def process_event(self, event):
        event_time = event.time
        event_type = event.type
        current_time = event_time

        if event_type == ARRIVAL:
            queue = self._select_queue()
            queue.process_event(event)

        elif event_type == DEPARTURE:
            queue_id = event.to
            queue = self.queues[queue_id]
            queue.process_event(event)

        else:
            print("Error: Unknown event type")

    def get_stats(self):
        num_customers_served = 0
        num_customers_dropped = 0
        max_t_end = 0.0
        total_wait_time = 0.0
        total_service_time = 0.0

        for server in self.queues:
            num_customers_served += server.num_customers_served
            num_customers_dropped += server.num_customers_dropped
            max_t_end = max(max_t_end, server.t_end)
            total_wait_time += server.total_wait_time
            total_service_time += server.total_service_time

        avg_wait_time = round(total_wait_time / num_customers_served, 4) if num_customers_served > 0 else 0
        avg_service_time = round(total_service_time / num_customers_served, 4) if num_customers_served > 0 else 0

        return num_customers_served, num_customers_dropped, round(max_t_end, 4), avg_wait_time, avg_service_time

    
    
    



def simulation(simulation_time, num_servers, p_list, queue_sized, service_rates, arrival_rate, seed=None):
    if (seed):
        random.seed(seed)

    # State variables
    current_time = 0.0
    event_list = []

    # init load balancer
    server = LoadBalancer(num_servers, p_list, queue_sized, service_rates, event_list)


    # set the first event
    event = Event(get_next_time(current_time, arrival_rate), ARRIVAL)
    heapq.heappush(event_list, event)

    while event_list:
        #get next event
        event = heapq.heappop(event_list)
        current_time = event.time

        # create new arrival event if needed
        if event.type == ARRIVAL and current_time < simulation_time: 
            next_arrival = get_next_time(current_time, arrival_rate)
            new_event = Event(next_arrival, ARRIVAL)
            heapq.heappush(event_list, new_event)


        server.process_event(event)

    # Results
    # num_customers_served, num_customers_dropped, max_t_end, avg_wait_time, avg_service_time = server.get_stats()

    # print(f"Number of customers served: {num_customers_served}")
    # print(f"Number of customers dropped: {num_customers_dropped}")
    # print(f"Max time: {max_t_end}")
    # print(f"Average wait time: {avg_wait_time}")
    # print(f"Average service time: {avg_service_time}")

    print(*server.get_stats())

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: ./simulator T M P_1 P_2 ... P_M λ Q_1 Q_2 ... Q_M μ_1 μ_2 ... μ_M")
        sys.exit(1)

    try:
        # Parameters from command line
        
        simulation_time = float(sys.argv[1])
        num_servers = int(sys.argv[2])
        p_list = [float(sys.argv[i]) for i in range(3, 3 + num_servers)]
        arrival_rate = float(sys.argv[3 + num_servers])
        queue_sized = [int(sys.argv[i]) for i in range(4 + num_servers, 4 + 2 * num_servers)]
        service_rates = [float(sys.argv[i]) for i in range(4 + 2 * num_servers, 4 + 3 * num_servers)]

        # Check lengths
        if len(p_list) != num_servers or len(queue_sized) != num_servers or len(service_rates) != num_servers:
            raise ValueError("The number of probabilities, queue sizes, and service rates must match the number of servers.")

        # Check if probabilities sum up to 1
        if not abs(sum(p_list) - 1.0) < 1e-6:
            raise ValueError("The probabilities must sum up to 1.")

    except Exception as e:
        print(f"Input error: {e}")
        print("Usage: ./simulator T M P_1 P_2 ... P_M λ Q_1 Q_2 ... Q_M μ_1 μ_2 ... μ_M")
        sys.exit(1)

    # parameters for testing
    # simulation_time = 50000
    # num_servers = 2
    # p_list = [0.5, 0.5]
    # queue_sized = [1000, 1000]
    # service_rates = [2.0, 2.0]
    # arrival_rate = 1.0

    simulation(simulation_time, num_servers, p_list, queue_sized, service_rates, arrival_rate)