# Input: arguments as list
# desc: check if all the argument is null, else return True
# output: True or False
def check_if_all_arg_is_not_null(args):
    for arg in args[:-1]:
        if arg is None:
            return False
    return True


# Input: arguments as list
# desc: check if any of the argument is not null
# output: True or False
def check_if_any_arg_is_not_null(args):
    for arg in args:
        if arg is not None:
            return True
    return False


# Input: load description, referenceNumberStructure
# desc: check if transportReference is valid
# output: True or False
def check_valid_transportReference(args):
    loadDescription = args[0]
    referenceNumberStructure = args[1]
    if loadDescription is not None:
        return True
    elif isinstance(referenceNumberStructure, list):
        for ref_num_stuc in referenceNumberStructure:
            if hasattr(ref_num_stuc, 'referenceNumberTypeCode'):
                if getattr(ref_num_stuc, 'referenceNumberTypeCode') == 'PRO_ID':
                    return True
    elif hasattr(referenceNumberStructure, 'referenceNumberTypeCode'):
        if getattr(referenceNumberStructure, 'referenceNumberTypeCode') == 'PRO_ID':
            return True

    return False

# Input: pickedSkids
# desc: return True if picked skids is not null and empty string.
# output: True or False


def check_if_valid_picked_skids(args):
    pickedSkids = args[0]
    if pickedSkids is None:
        return False
    if pickedSkids == "":
        return False
    return True

# Input: droppedSkids
# desc: return True if dropped skids is not null and empty string.
# output: True or False


def check_if_valid_dropped_skids(args):
    droppedSkids = args[0]
    if droppedSkids is None:
        return False
    if droppedSkids == "":
        return False
    return True

# Input: shippingLocationCode, docks
# desc: return True if dock commitment id is valid
# output: True or False


def check_if_valid_dock_commitment_id(args):
    shippingLocationCode = args[0]
    docks = args[1]
    if docks is None:
        return False

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode',None) == shippingLocationCode:
                if getattr(getattr(dock, 'dockCommitment', [None])[0], 'systemDockCommitmentID', None) is not None:
                    return True

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            if getattr(getattr(docks, 'dockCommitment', [None])[0], 'systemDockCommitmentID', None) is not None:
                return True

    return False


# Input: loadReferenceNumberStructures, eventName, loadWHStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or LoadRoutedRatedScheduled or load warehouse status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_load_ref_update_shipment_leg(args):
    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "transportLoadShipmentLegOperation", "LoadRoutedRatedScheduled"]:
        return True
    elif eventName == "LoadTenderAccepted":
        return True
    elif eventName == "LoadTenderCancelled":
        return True
    return False


# Input: loadReferenceNumberStructures, eventName, loadWHStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or LoadRoutedRatedScheduled or load warehouse status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_load_ref_update_tl(args):
    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadStopAppointment", "LoadRoutedRatedScheduled"]:
        return True
    elif eventName == "LoadTenderAccepted":
        return True
    elif eventName == "LoadTenderCancelled":
        return True
    return False


# Input: stopReferenceNumberStructures, eventName, stopStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or LoadRoutedRatedScheduled or stop status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_stop_ref_update_shipment_leg(args):
    stopReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(stopReferenceNumberStructures, list):
        for RNS in stopReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "transportLoadShipmentLegOperation", "LoadRoutedRatedScheduled"]:
        return True
    elif eventName == "LoadTenderAccepted":
        return True
    elif eventName == "LoadTenderCancelled":
        return True
    return False


# Input: stopReferenceNumberStructures, eventName, stopStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or LoadRoutedRatedScheduled or stop status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_stop_ref_update_tl(args):
    stopReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(stopReferenceNumberStructures, list):
        for RNS in stopReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadRoutedRatedScheduled", "LoadStopAppointment"]:
        return True
    elif eventName == "LoadTenderAccepted":
        return True
    elif eventName == "LoadTenderCancelled":
        return True
    return False

# Input: lastComputedArrivalDateTime, lastComputedDepartureDateTime, appointment, countOfShipmentsAtStop
# desc: return True if stop logistic event is unloading/ loading by comparing count of Shipments at Stop
# output: True or False
def check_if_valid_loading_unloading_logistic_event(args):
    lastComputedArrivalDateTime = args[0]
    lastComputedDepartureDateTime = args[1]
    appointment = args[2]
    countOfShipmentsAtStop = args[3]
    is_lastComputedDateTime = lastComputedDepartureDateTime is not None and lastComputedArrivalDateTime is not None
    if (is_lastComputedDateTime or appointment is not None) and countOfShipmentsAtStop is not None:
        if int(countOfShipmentsAtStop) > 0:
            return True
    return False


# Input: stops, shipmentLegNumber
# desc: return True if Event is last shipment remove from load or stop
# output: True or False
def check_if_last_shipment_remove(args):
    stops = args[0]
    shipmentLegNumber = args[1]
    stopLocations = []
    if stops is None:
        return True

    if isinstance(stops, list):
        for stop in stops:
            shippingLocationCode = getattr(stop, "shippingLocationCode", None)
            stopLocations.append(shippingLocationCode)
    else:
        shippingLocationCode = getattr(stops, "shippingLocationCode", None)
        stopLocations.append(shippingLocationCode)

    if shipmentLegNumber is not None and shipmentLegNumber not in stopLocations:
        return True

    return False
