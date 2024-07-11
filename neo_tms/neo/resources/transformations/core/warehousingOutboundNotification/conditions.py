def update_stop_planning_status_in_tms(*args):
    """
     Update the stop planning status in the Transportation Management System (TMS).
     This function checks if a transport load ID is provided and updates the planning status accordingly.
    :param args: list of a tuple of item(currentLoadOperationalStatusEnumVal, message hdr info)
    :return: True if transport_load_id is present else False
    """
    transport_load_id = args[0][0]
    if transport_load_id:
        return True
    else:
        return False


def validate_won_product_picked(*args):
    """
    Validate the incoming WON product picked messages

    This function checks if the incoming product picked message is having shipment container, extracted containers
    and valid ids

    :param args: A variable length argument list. It is assumed that the arguments are in the following order:
                - validate_ids: True or False based on tms_validate_won_ids function in operations.py
                - vars_containers: details containing extracted containers
                - currentContainer: details containing all the container information
    """
    validate_ids = args[0][0]
    vars_containers = args[0][1]
    currentContainer = args[0][2]
    if validate_ids:
        if vars_containers and currentContainer:
            return True
    return False
