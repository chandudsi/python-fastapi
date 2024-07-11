def check_load_status_and_load_id(*args):
    """
    This function is used checking the currentLoadOperationalStatusEnumVal is not S_OPEN shipment which will be used
    while a remove a shipment from planned load attempted before deleting/cancelling shipment for order Release
    :param args: list of a tuple of item(currentLoadOperationalStatusEnumVal, message hdr info)
    :return: True if currentLoadOperationalStatusEnumVal is not S_OPEN else False
    """
    currentLoadOperationalStatusEnumVal = args[0][0]
    if currentLoadOperationalStatusEnumVal and currentLoadOperationalStatusEnumVal != "S_OPEN":
        return True
    return False
