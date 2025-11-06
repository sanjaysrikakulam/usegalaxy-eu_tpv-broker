from math import cos, asin, sqrt


def distance(lat1: float, lon1: float, lat2: float, lon2: float):
    p = 0.017453292519943295
    hav = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(hav))


def get_object_store(dataset_attributes):
    """Extract the object store id from the dataset attributes"""
    object_store = []
    for value in dataset_attributes.values():
        if value.object_store_id:
            object_store.append(value.object_store_id)

    if len(set(object_store)) == 1:
        object_store = [object_store[0]]

    return object_store


def closest_destination(destination, objectstores, dataset_attributes) -> float:
    """
    Calculates the minimum distance between a given destination and the object store(s).
    """
    object_store = get_object_store(dataset_attributes)

    # If there is no object store, return infinity
    if not object_store:
        return float('inf')

    d_lat, d_lon = destination['latitude'], destination['longitude']
    min_distance = float('inf')

    # Calculate distance to each object store and keep the minimum
    for object_store_id in object_store:
        object_store_info = objectstores.get(object_store_id)
        if object_store_info:
            o_lat, o_lon = object_store_info.latitude, object_store_info.longitude
            min_distance = min(min_distance, distance(o_lat, o_lon, d_lat, d_lon))

    return min_distance


def calculate_matching_score(destination: dict) -> float:
    """
    Calculate the matching score between a job and a destination
    """
    median_waiting_time = destination.get('dest_tool_median_queue_time', None)
    queue_size = destination.get('dest_queue_count', 1)
    median_running_time = destination.get('dest_tool_median_run_time', None)
    running_jobs = destination.get('dest_run_count', 1)

    # Queue matching factor (qm).
    if median_waiting_time > 0 and queue_size > 0:
        qm = 1 / (median_waiting_time * queue_size)
    else:
        qm = float('inf')

    # Compute matching factor (cm).
    if median_running_time > 0 and running_jobs > 0:
        cm = 1 / (median_running_time * running_jobs)
    else:
        cm = float('inf')

    # Final matching score
    return qm + cm


def get_sorted_destinations(job_requirements, destinations: list, objectstores, dataset_attributes) -> list:
    """
    Sorts the destinations based on the matching score and distance to the input data location.
    The sorting considers a histogram of free resources (CPU and memory) rather than aggregated values.
    """
    cpu_required = job_requirements.cores
    memory_required = job_requirements.memory * 1024  # The memory in TPV is in GB, so we do a conversion to MB because the cluster metrics are in MB.

    # Filter out destinations that can't meet basic requirements based on the "real-time" data
    viable_destinations = []
    for dest in destinations:
        # Check if the destination_status is 'online'
        if dest['dest_status'] == 'online':
            cpu_histogram = dest.get('dest_cpu_histogram', {})
            memory_histogram = dest.get('dest_memory_histogram', {})

            # Check if any machine in the cluster has enough free CPUs and memory
            has_sufficient_cpu_resources = False
            has_sufficient_memory_resources = False
            for free_cpus, count in cpu_histogram.items():
                free_cpus = int(free_cpus)
                if free_cpus >= cpu_required:
                    has_sufficient_cpu_resources = True
                    break

            for free_memory, count in memory_histogram.items():
                free_memory = int(free_memory)
                if free_memory >= memory_required:
                    has_sufficient_memory_resources = True
                    break

            # Only consider this destination if it has enough resources
            if has_sufficient_cpu_resources and has_sufficient_memory_resources:
                # Calculate the distance to the input data location
                dest['distance_to_data'] = closest_destination(dest, objectstores, dataset_attributes)
                viable_destinations.append(dest)

    # Fallback case if no viable destinations are found
    if not viable_destinations:
        online_destinations = []
        for dest in destinations:
            if dest.get('dest_status') == 'online':
                dest['distance_to_data'] = closest_destination(dest, objectstores, dataset_attributes)
                online_destinations.append(dest)

        sorted_destinations = sorted(online_destinations, key=lambda x: x['distance_to_data'])
        return [dest['destination_id'] for dest in sorted_destinations]

    # Calculate matching scores for each viable destination
    for dest in viable_destinations:
        dest['matching_score'] = calculate_matching_score(dest)
        #ToDo Calculate distance to input data location as well for possible secondary sorting to include both
        # matching score and distance.
        # dest['distance_to_data'] = closest_destination(dest, objectstores, dataset_attributes)

    # Sort by matching score (descending)
    viable_destinations.sort(key=lambda x: x['matching_score'], reverse=True)

    # ToDo: Consider distance as a secondary sorting criterion and below is a possible implementation
    # viable_destinations.sort(key=lambda x: ( -x['matching_score'], x['distance_to_data']))

    sorted_destinations = [dest['destination_id'] for dest in viable_destinations]
    return sorted_destinations
