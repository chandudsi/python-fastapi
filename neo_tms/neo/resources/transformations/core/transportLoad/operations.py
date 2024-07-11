from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger
from datetime import datetime
import pytz

RECEIVER_PREFIX = adapter_cps_provider.get_properties().get('bydm', {}).get("receivers", {}).get("prefix", "") +\
                  adapter_cps_provider.get_properties().get('bydm', {}).get("concat_symbol", '.')

def generate_uuid(*args, **kwargs):
    from uuid import uuid4
    return str(uuid4())


def get_current_timestamp_iso8601(*args, **kwargs):
    from pendulum import now
    return now().isoformat(timespec='milliseconds')


# Transport Load Custom Function - START

def generate_dynamic_receivers_transport_load(args):

    """
    This function evaluates receivers for a Transport Load
    params- list of stops in transport Load
    :return: List of receivers
    """

    qualifier_ = f"operations_custom.get_TL_reciever"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating list of receiver for Transport Load"

    logger.info(log_text, message_hdr_info)

    result = []
    stops = args[0]
    if stops is None:
        return None

    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, "countOfShipmentsPickedAtStop") is not None:
                if getattr(stop, "countOfShipmentsPickedAtStop") > 0:
                    StopStatusEnumVal = getattr(stop, 'stopStatusEnumVal')
                    not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                    if StopStatusEnumVal not in not_in_list:
                        if hasattr(stop, "shippingLocationCode"):
                            shippingLocationCode = RECEIVER_PREFIX + getattr(stop, "shippingLocationCode")
                            result.append(shippingLocationCode)
    else:
        if hasattr(stops, "countOfShipmentsPickedAtStop") is not None:
            if getattr(stops, "countOfShipmentsPickedAtStop") > 0:
                StopStatusEnumVal = getattr(stops, 'stopStatusEnumVal')
                not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                if StopStatusEnumVal not in not_in_list:
                    if hasattr(stops, "shippingLocationCode"):
                        shippingLocationCode = RECEIVER_PREFIX + getattr(stops, "shippingLocationCode")
                        result.append(shippingLocationCode)

    return result


def get_TL_messageVersion(args):
    """
    This function evaluates message version from property for Transport Load
    params- message version defined in property
    :return: message version
    """

    qualifier_ = f"operations_custom.get_TL_messageVersion"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating message version from property for Transport Load"
    logger.info(log_text, message_hdr_info)

    messageVersion = args[0]
    return messageVersion


def get_gs1_location_offset(location_offset, default_tz):
    """
    This function adds two location offset and returns the resulted offset
    params- location_offset: business offset fetched for the shiping location
    params- default_tz: time zone of TM Adapter
    :return: resultant offset
    """
    if location_offset is None:
        location_offset = 0

    timezones = pytz.all_timezones

    if default_tz not in timezones:
        default_tz = "UTC"

    tm_tz = pytz.timezone(default_tz)
    tm_offset_minutes = tm_tz.utcoffset(datetime.now()).total_seconds() // 60
    location_offset = float(location_offset) * 60

    location_offset_in_minutes = float(tm_offset_minutes) + float(location_offset)
    location_offset_in_hours = location_offset_in_minutes//60
    remainder_minutes = location_offset_in_minutes % 60
    offset_sign = '-' if location_offset_in_hours < 0 else '+'
    offset_hour = "{:02}".format(int(abs(location_offset_in_hours)))
    offset_min = "{:02}".format(int(remainder_minutes))

    return f"{offset_sign}{offset_hour}:{offset_min}"


def getDateComponentForTL_logisticEvent_From(args):
    """
    This function evaluates date component of logistic Event "From" for Transport Load
    params- appointment, lastComputedArrivalDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getDateComponentForTL_logisticEvent_From"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating date component of logistic Event 'From' for Transport Load"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_From(args):
    """
    This function evaluates time component of logistic Event "From" for Transport Load
    params- appointment, lastComputedArrivalDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getTimeComponentForTL_logisticEvent_From"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating time component of logistic Event 'From' for Transport Load"
    logger.info(log_text, message_hdr_info)

    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    dock_enrichment = args[2]
    stop_location_code = args [3]
    tm_defaultTZ = args[4]
    location_offset = 0

    if dock_enrichment is not None and stop_location_code is not None and isinstance(dock_enrichment, list):
        for dock in dock_enrichment:
            dockLocationCode = get_value(dock, "shippingLocationCode")
            if stop_location_code == dockLocationCode:
                location_offset = get_value(dock, 'businessHours.timeZoneOffset', 0)

    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None

    offset = get_gs1_location_offset(location_offset, tm_defaultTZ)
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time + offset


def getDateComponentForTL_logisticEvent_To(args):
    """
    This function evaluates Date component of logistic Event "From" for Transport Load
    params- appointment, lastComputedDepartureDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getDateComponentForTL_logisticEvent_To"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating Date component of logistic Event 'To' for Transport Load"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedDepartureDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedDepartureDateTime
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_To(args):
    """
    This function evaluates Time component of logistic Event "To" for Transport Load
    params- appointment, lastComputedDepartureDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getTimeComponentForTL_logisticEvent_To"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating Time component of logistic Event 'To' for Transport Load"
    logger.info(log_text, message_hdr_info)

    appointment = args[1]
    lastComputedArrivalDateTime = args[0]

    dock_enrichment = args[2]
    stop_location_code = args [3]
    tm_defaultTZ = args[4]
    location_offset = 0

    if dock_enrichment is not None and stop_location_code is not None and isinstance(dock_enrichment, list):
        for dock in dock_enrichment:
            dockLocationCode = get_value(dock, "shippingLocationCode")
            if stop_location_code == dockLocationCode:
                location_offset = get_value(dock, 'businessHours.timeZoneOffset', 0)

    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None

    offset = get_gs1_location_offset(location_offset, tm_defaultTZ)
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time + offset


def is_multi_leg_shipment(args):
    """
    This function evaluates shipment if it is multi leg for Transport Load
    """

    qualifier_ = f"operations_custom.is_multi_leg_shipment"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating shipment if it is multi leg for Transport Load"
    logger.info(log_text, message_hdr_info)

    numberOfShipmentLeg = args[0]

    if numberOfShipmentLeg is None:
        numberOfShipmentLeg = 0

    if numberOfShipmentLeg > 1:
        return True
    else:
        return False


def is_cross_dock_leg(args):
    """
    This function evaluates if it is cross dock leg for Transport Load
    params- numberOfShipmentLeg
    :return: bool
    """

    qualifier_ = f"operations_custom.is_cross_dock_leg"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating shipment Leg if it is cross dock leg for Transport Load"
    logger.info(log_text, message_hdr_info)

    numberOfShipmentLeg = args[0]
    SL_shipFromLocationCode = args[1]
    shipFromLocationCode = args[2]

    if numberOfShipmentLeg > 0 and (SL_shipFromLocationCode != shipFromLocationCode):
        return True
    else:
        return False


def get_date_component(args):
    """
    This function evaluates date component from date time
    params- numberOfShipmentLeg
    :return: bool
    """

    qualifier_ = f"operations_custom.get_date_component"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating date component from date time"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def get_time_component(args):
    """
    This function evaluates time component from date time
    """

    qualifier_ = f"operations_custom.get_time_component"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating time component from date time"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time


def get_referenceNumberStructure_by_type(args):
    """
    This function evaluates reference structure by type
    """

    qualifier_ = f"operations_custom.get_referenceNumberStructure_by_type"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference structure by type."
    logger.info(log_text, message_hdr_info)

    referenceNumberStructures = args[0]
    typecode = args[1]
    if isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_item_id_for_transactionalTradeItem(args):
    """
    This function evaluates item Id for transactional Trade Item
    """

    qualifier_ = f"operations_custom.get_item_id_for_transactionalTradeItem"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating item Id for transactional Trade Item."
    logger.info(log_text, message_hdr_info)

    TransportOrderInfo_ProductNumberCode = args[0]
    ShipmentItem_ItemNumber = args[1]
    ItemNumber = args[2]
    referenceNumberStructures = args[3]
    typecode = args[4]
    if TransportOrderInfo_ProductNumberCode is not None:
        return TransportOrderInfo_ProductNumberCode
    elif ShipmentItem_ItemNumber is not None:
        return ShipmentItem_ItemNumber
    elif ItemNumber is not None:
        return ItemNumber
    elif isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_pickupShipmentReference_for_stops_TL(args):
    """
    This function evaluates pickup Shipment Reference from Stops
    """

    qualifier_ = f"operations_custom.get_pickupShipmentReference_for_stops_TL"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating pickup Shipment Reference from Stops."
    logger.info(log_text, message_hdr_info)

    pickShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if pickShipmentLegID is not None:
        if isinstance(pickShipmentLegID, list):
            for id in pickShipmentLegID:
                if isinstance(ShipmentLegIDs, list):
                    for S_id in ShipmentLegIDs:
                        ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                        if id == ShipmentLegID:
                            output = {}
                            output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                            output['additionalShipmentId'] = [
                                {
                                    "typeCode": "SHIPPER_ASSIGNED",
                                    "value": "00000000000000000"
                                }
                            ]
                            output_list.append(output)
                            break
        return output_list
    else:
        return None


def get_dropoffShipmentReference_for_stops_TL(args):
    """
    This function evaluates dropOff Shipment Reference from Stops
    """

    qualifier_ = f"operations_custom.get_dropoffShipmentReference_for_stops_TL"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dropOff Shipment Reference from Stops."
    logger.info(log_text, message_hdr_info)

    dropoffShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if dropoffShipmentLegID is not None:
        if isinstance(dropoffShipmentLegID, list):
            for id in dropoffShipmentLegID:
                if isinstance(ShipmentLegIDs, list):
                    for S_id in ShipmentLegIDs:
                        ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                        if id == ShipmentLegID:
                            output = {}
                            output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                            output['additionalShipmentId'] = [
                                {
                                    "typeCode": "SHIPPER_ASSIGNED",
                                    "value": "00000000000000000"
                                }
                            ]
                            output_list.append(output)
                            break
        return output_list
    else:
        return None


def get_entityId_transportReference(args):
    """
    This function evaluates entityId for transport Reference
    """

    qualifier_ = f"operations_custom.get_entityId_transportReference"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating entityId for transport Reference."
    logger.info(log_text, message_hdr_info)

    loadDescription = args[0]
    referenceNumberStructure = args[1]
    if loadDescription is not None:
        return loadDescription
    elif isinstance(referenceNumberStructure, list):
        for ref_num_stuc in referenceNumberStructure:
            if hasattr(ref_num_stuc, 'referenceNumberTypeCode'):
                if getattr(ref_num_stuc, 'referenceNumberTypeCode') == 'PRO_ID':
                    return getattr(ref_num_stuc, 'referenceNumber')
    elif hasattr(referenceNumberStructure, 'referenceNumberTypeCode'):
        if getattr(referenceNumberStructure, 'referenceNumberTypeCode') == 'PRO_ID':
            return getattr(referenceNumberStructure, 'referenceNumber')

    return None


def get_total_pickup_stops(args):
    """
    This function evaluates no. of pickup Stops
    """

    qualifier_ = f"operations_custom.get_total_pickup_stops"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating no. of pickup Stops."
    logger.info(log_text, message_hdr_info)

    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsPickedAtStop'):
                if getattr(stop, 'countOfShipmentsPickedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsPickedAtStop'):
            if getattr(stops, 'countOfShipmentsPickedAtStop') > 0:
                counter += 1
    return counter


def get_total_dropoff_stops(args):
    """
    This function evaluates no. of dropOff Stops
    """

    qualifier_ = f"operations_custom.get_total_dropoff_stops"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating no. of dropOff Stops."
    logger.info(log_text, message_hdr_info)

    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsDroppedAtStop'):
                if getattr(stop, 'countOfShipmentsDroppedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsDroppedAtStop'):
            if getattr(stops, 'countOfShipmentsDroppedAtStop') > 0:
                counter += 1
    return counter


def get_arg_as_list(args):
    """
    This function evaluates all the argument and return as a list
    """

    qualifier_ = f"operations_custom.get_arg_as_list"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating all the argument and return as a list."
    logger.info(log_text, message_hdr_info)

    output = []
    for arg in args[:-1]:
        if arg is not None:
            output.append(arg)

    if len(output) == 0:
        return None
    return output


def get_dock_slot_identifier(args):
    """
    This function evaluates dock slot identified(sublocationId) for Stop
    """

    qualifier_ = f"operations_custom.get_dock_slot_identifier"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock slot identified(sublocationId) for Stop."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockSlotIdentifier', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockSlotIdentifier', None)


def get_dockId(args):
    """
    This function evaluates dock Id
    """

    qualifier_ = f"operations_custom.get_dockId"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock Id."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockCode', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockCode', None)


def get_dock_type_code(args):
    """
    This function evaluates dock type code
    """

    qualifier_ = f"operations_custom.get_dock_type_code"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock type code."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'DockTypeEnumVal', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'DockTypeEnumVal', None)


def get_systemDockCommitmentID(args):
    """
    This function evaluates system Dock CommitmentID
    """

    qualifier_ = f"operations_custom.get_systemDockCommitmentID"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating system Dock CommitmentID."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return False

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                if getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                    return getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID')

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            if getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                return getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID')

    return False


def get_document_action_code_transport_load(args):
    """
    This function evaluates document Action Code
    """

    qualifier_ = f"operations_custom.get_document_action_code_transport_load"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating document Action Code."
    logger.info(log_text, message_hdr_info)

    start_status = args[0]
    event_name = args[1]
    current_load_status = args[2]
    visibility_options = args[3]
    message_header_info = args[-1]
    document_action_code = None
    list_ = visibility_options.get(start_status, {}).get('VisibilityOption', [])

    filtered_item = list(filter(lambda visibility_option: visibility_option.get(
                "CurrentLoadStatus") == current_load_status
                                                                  and visibility_option.get(
                "EventName") == event_name,
                                        list_))
    if len(filtered_item) > 0:
        document_action_code = filtered_item[0].get("DocumentOperationCode")

    print("In Custom Function")
    print(message_header_info)

    return document_action_code


# Transport Load Custom Function - END

# TransporLoad Reference Number Update - START


def get_reference_number_for_load_tl(args):
    """
    This function evaluates reference no for load TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference no for load TL reference update."
    logger.info(log_text, message_hdr_info)

    stops = args[0]
    eventName = args[1]
    referenceNumber = []
    if eventName in ['LoadTenderCancelled']:
        return None
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'shippingLocationCode'):
                if int(getattr(stop, "countOfShipmentsPickedAtStop")) > 0:
                    referenceNumber.append(f"{str(getattr(stop, 'shippingLocationCode'))}-AWARE")

    else:
        if hasattr(stops, 'shippingLocationCode'):
            if int(getattr(stops, "countOfShipmentsPickedAtStop")) > 0:
                referenceNumber.append(f"{str(getattr(stops, 'shippingLocationCode'))}-AWARE")

    return ','.join(referenceNumber)


def get_reference_number_action_enum_for_load_tl(args):
    """
    This function evaluates reference Number Action EnumVal for TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_action_enum_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference Number Action EnumVal for TL reference update."
    logger.info(log_text, message_hdr_info)

    eventName = args[0]
    if eventName == "LoadTenderAccepted":
        return "AT_ADD"
    elif eventName in ["LoadTenderUpdated", "LoadRoutedRatedScheduled"]:
        return "AT_UPDATE"
    elif eventName == "LoadTenderCancelled":
        return "AT_DELETE"
    return None


def get_system_reference_id_for_load_tl(args):
    """
    This function evaluates reference Id for TL reference update
    """

    qualifier_ = f"operations_custom.get_system_reference_id_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference Id for TL reference update."
    logger.info(log_text, message_hdr_info)

    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled", "LoadRoutedRatedScheduled"]:
        return loadWHStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_system_reference_id_stop_load_tl(args):
    """
    This function evaluates system reference Id for TL reference update
    """

    qualifier_ = f"operations_custom.get_system_reference_id_stop_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating system reference Id for TL reference update."
    logger.info(log_text, message_hdr_info)

    ReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(ReferenceNumberStructures, list):
        for RNS in ReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled", "LoadRoutedRatedScheduled"]:
        return stopStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_reference_number_for_stop_tl(args):
    """
    This function evaluates reference number for stop for TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_for_stop_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference number for stop for TL reference update."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    eventName = args[1]
    if eventName in ['LoadTenderCancelled']:
        return None
    return shippingLocationCode + "-AWARE"


def get_shipment_transportReference(args):
    """
    This function evaluates transportReference for shipment in TL
    """

    qualifier_ = f"operations_custom.get_shipment_transportReference"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating transportReference for shipment in TL."
    logger.info(log_text, message_hdr_info)

    currentShipmentLeg = args[0]

    transportReference = []
    OrderConsolidationGroupID = get_value(currentShipmentLeg, "shipment.container.transportOrderInfo.orderConsolidationGroupID", None)

    if OrderConsolidationGroupID is not None:
        transportReference.append({
                "entityId": OrderConsolidationGroupID,
                "typeCode": 'ECN'
        })

    OrderNumberCode = get_value(currentShipmentLeg, "shipment.container.transportOrderInfo.orderNumberCode", None)
    ReferenceNumberStructure = get_value(currentShipmentLeg, "shipment.referenceNumberStructure")

    if isinstance(ReferenceNumberStructure, list):
        ReferenceNumber = None
        for ref_num in ReferenceNumberStructure:
            typecode = get_value(ref_num, "referenceNumberTypeCode")
            if typecode == "WM_ORDNUM_":
                ReferenceNumber = get_value(ref_num, "referenceNumber")

        if ReferenceNumber is not None and OrderNumberCode is None:
            transportReference.append({
                "entityId": ReferenceNumber,
                "typeCode": 'ON'
            })
    if len(transportReference)>0:
        return transportReference
    else:
        return None

# Shipment Leg Operation - Transport Load START

def generate_dynamic_receivers_transport_load_shipment_leg(args):
    """
    This function evaluates receivers for a Transport Load
    params- list of stops in transport Load , ShipmentLegOriginCode for shipment
    :return: List of receivers
    """

    qualifier_ = f"operations_custom.get_TL_reciever"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating list of receiver for Transport Load"

    logger.info(log_text, message_hdr_info)

    result = []
    stops = args[0]
    ShipmentLegOriginCode = args[1]
    affectedOriginLocation = None

    if stops is not None:
        if isinstance(stops, list):
            for stop in stops:
                if hasattr(stop, "countOfShipmentsPickedAtStop") is not None:
                    if getattr(stop, "countOfShipmentsPickedAtStop") > 0:
                        StopStatusEnumVal = getattr(stop, 'stopStatusEnumVal')
                        not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                        if StopStatusEnumVal not in not_in_list:
                            if hasattr(stop, "shippingLocationCode"):
                                shippingLocationCode = RECEIVER_PREFIX + getattr(stop, "shippingLocationCode")
                                result.append(shippingLocationCode)
        else:
            if hasattr(stops, "countOfShipmentsPickedAtStop") is not None:
                if getattr(stops, "countOfShipmentsPickedAtStop") > 0:
                    StopStatusEnumVal = getattr(stops, 'stopStatusEnumVal')
                    not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                    if StopStatusEnumVal not in not_in_list:
                        if hasattr(stops, "shippingLocationCode"):
                            shippingLocationCode = RECEIVER_PREFIX + getattr(stops, "shippingLocationCode")
                            result.append(shippingLocationCode)

    if ShipmentLegOriginCode is not None:
        affectedOriginLocation = RECEIVER_PREFIX + str(ShipmentLegOriginCode)

    if affectedOriginLocation is not None and affectedOriginLocation not in result:
        result.append(affectedOriginLocation)

    return result


def generate_affectedOriginLocation_shipment_leg(args):
    """
    This function evaluates affectedOriginLocation where delete occurs for shipment and Load
     for a Transport Load
    params- list of stops in transport Load , ShipmentLegOriginCode for shipment
    :return: List of receivers
    """

    qualifier_ = "operations_custom.generate_affectedOriginLocation"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating affected Origin Location for Transport Load"

    logger.info(log_text, message_hdr_info)

    result = []
    stops = args[0]
    ShipmentLegOriginCode = args[1]
    TL_parsing_enabled = args[2]
    affectedOriginLocation = None

    if TL_parsing_enabled is None or TL_parsing_enabled is False:
        return None

    if stops is not None:
        if isinstance(stops, list):
            for stop in stops:
                if hasattr(stop, "countOfShipmentsPickedAtStop") is not None:
                    if getattr(stop, "countOfShipmentsPickedAtStop") > 0:
                        StopStatusEnumVal = getattr(stop, 'stopStatusEnumVal')
                        not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                        if StopStatusEnumVal not in not_in_list:
                            if hasattr(stop, "shippingLocationCode"):
                                shippingLocationCode = RECEIVER_PREFIX + getattr(stop, "shippingLocationCode")
                                result.append(shippingLocationCode)
        else:
            if hasattr(stops, "countOfShipmentsPickedAtStop") is not None:
                if getattr(stops, "countOfShipmentsPickedAtStop") > 0:
                    StopStatusEnumVal = getattr(stops, 'stopStatusEnumVal')
                    not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                    if StopStatusEnumVal not in not_in_list:
                        if hasattr(stops, "shippingLocationCode"):
                            shippingLocationCode = RECEIVER_PREFIX + getattr(stops, "shippingLocationCode")
                            result.append(shippingLocationCode)

    if ShipmentLegOriginCode is not None:
        affectedOriginLocation = RECEIVER_PREFIX + str(ShipmentLegOriginCode)

    if affectedOriginLocation is not None and affectedOriginLocation not in result:
        return ShipmentLegOriginCode
    return None


def get_TL_messageVersion_shipment_leg(args):
    """
    This function evaluates message version from property for Transport Load
    params- message version defined in property
    :return: message version
    """

    qualifier_ = f"operations_custom.get_TL_messageVersion"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating message version from property for Transport Load"
    logger.info(log_text, message_hdr_info)

    messageVersion = args[0]
    return messageVersion


def get_gs1_location_offset_shipment_leg(location_offset, default_tz):
    """
    This function adds two location offset and returns the resulted offset
    params- location_offset: business offset fetched for the shiping location
    params- default_tz: time zone of TM Adapter
    :return: resultant offset
    """
    if location_offset is None:
        location_offset = 0

    timezones = pytz.all_timezones

    if default_tz not in timezones:
        default_tz = "UTC"

    tm_tz = pytz.timezone(default_tz)
    tm_offset_minutes = tm_tz.utcoffset(datetime.now()).total_seconds() // 60
    location_offset = float(location_offset) * 60

    location_offset_in_minutes = float(tm_offset_minutes) + float(location_offset)
    location_offset_in_hours = location_offset_in_minutes // 60
    remainder_minutes = location_offset_in_minutes % 60
    offset_sign = '-' if location_offset_in_hours < 0 else '+'
    offset_hour = "{:02}".format(int(abs(location_offset_in_hours)))
    offset_min = "{:02}".format(int(remainder_minutes))

    return f"{offset_sign}{offset_hour}:{offset_min}"


def getDateComponentForTL_logisticEvent_From_shipment_leg(args):
    """
    This function evaluates date component of logistic Event "From" for Transport Load
    params- appointment, lastComputedArrivalDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getDateComponentForTL_logisticEvent_From"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating date component of logistic Event 'From' for Transport Load"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_From_shipment_leg(args):
    """
    This function evaluates time component of logistic Event "From" for Transport Load
    params- appointment, lastComputedArrivalDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getTimeComponentForTL_logisticEvent_From"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating time component of logistic Event 'From' for Transport Load"
    logger.info(log_text, message_hdr_info)

    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    dock_enrichment = args[2]
    stop_location_code = args[3]
    tm_defaultTZ = args[4]
    location_offset = 0

    if dock_enrichment is not None and stop_location_code is not None and isinstance(dock_enrichment, list):
        for dock in dock_enrichment:
            dockLocationCode = get_value(dock, "shippingLocationCode")
            if stop_location_code == dockLocationCode:
                location_offset = get_value(dock, 'businessHours.timeZoneOffset', 0)

    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None

    offset = get_gs1_location_offset_shipment_leg(location_offset, tm_defaultTZ)
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time + offset


def getDateComponentForTL_logisticEvent_To_shipment_leg(args):
    """
    This function evaluates Date component of logistic Event "From" for Transport Load
    params- appointment, lastComputedDepartureDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getDateComponentForTL_logisticEvent_To"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating Date component of logistic Event 'To' for Transport Load"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedDepartureDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedDepartureDateTime
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_To_shipment_leg(args):
    """
    This function evaluates Time component of logistic Event "To" for Transport Load
    params- appointment, lastComputedDepartureDateTime
    :return: message version
    """

    qualifier_ = f"operations_custom.getTimeComponentForTL_logisticEvent_To"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating Time component of logistic Event 'To' for Transport Load"
    logger.info(log_text, message_hdr_info)

    appointment = args[1]
    lastComputedArrivalDateTime = args[0]

    dock_enrichment = args[2]
    stop_location_code = args[3]
    tm_defaultTZ = args[4]
    location_offset = 0

    if dock_enrichment is not None and stop_location_code is not None and isinstance(dock_enrichment, list):
        for dock in dock_enrichment:
            dockLocationCode = get_value(dock, "shippingLocationCode")
            if stop_location_code == dockLocationCode:
                location_offset = get_value(dock, 'businessHours.timeZoneOffset', 0)

    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    if dateTimeValue is None:
        return None

    offset = get_gs1_location_offset_shipment_leg(location_offset, tm_defaultTZ)
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time + offset


def is_multi_leg_shipment_shipment_leg(args):
    """
    This function evaluates shipment if it is multi leg for Transport Load
    """

    qualifier_ = f"operations_custom.is_multi_leg_shipment"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating shipment if it is multi leg for Transport Load"
    logger.info(log_text, message_hdr_info)

    numberOfShipmentLeg = args[0]

    if numberOfShipmentLeg is None:
        numberOfShipmentLeg = 0

    if numberOfShipmentLeg > 1:
        return True
    else:
        return False


def is_cross_dock_leg_shipment_leg(args):
    """
    This function evaluates if it is cross dock leg for Transport Load
    params- numberOfShipmentLeg
    :return: bool
    """

    qualifier_ = f"operations_custom.is_cross_dock_leg"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating shipment Leg if it is cross dock leg for Transport Load"
    logger.info(log_text, message_hdr_info)

    numberOfShipmentLeg = args[0]
    SL_shipFromLocationCode = args[1]
    shipFromLocationCode = args[2]

    if numberOfShipmentLeg > 0 and (SL_shipFromLocationCode != shipFromLocationCode):
        return True
    else:
        return False


def get_date_component_shipment_leg(args):
    """
    This function evaluates date component from date time
    params- numberOfShipmentLeg
    :return: bool
    """

    qualifier_ = f"operations_custom.get_date_component"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating date component from date time"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def get_time_component_shipment_leg(args):
    """
    This function evaluates time component from date time
    """

    qualifier_ = f"operations_custom.get_time_component"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating time component from date time"
    logger.info(log_text, message_hdr_info)

    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    if dateTimeValue is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time


def get_referenceNumberStructure_by_type_shipment_leg(args):
    """
    This function evaluates reference structure by type
    """

    qualifier_ = f"operations_custom.get_referenceNumberStructure_by_type"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference structure by type."
    logger.info(log_text, message_hdr_info)

    referenceNumberStructures = args[0]
    typecode = args[1]
    if isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_item_id_for_transactionalTradeItem_shipment_leg(args):
    """
    This function evaluates item Id for transactional Trade Item
    """

    qualifier_ = f"operations_custom.get_item_id_for_transactionalTradeItem"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating item Id for transactional Trade Item."
    logger.info(log_text, message_hdr_info)

    TransportOrderInfo_ProductNumberCode = args[0]
    ShipmentItem_ItemNumber = args[1]
    ItemNumber = args[2]
    referenceNumberStructures = args[3]
    typecode = args[4]
    if TransportOrderInfo_ProductNumberCode is not None:
        return TransportOrderInfo_ProductNumberCode
    elif ShipmentItem_ItemNumber is not None:
        return ShipmentItem_ItemNumber
    elif ItemNumber is not None:
        return ItemNumber
    elif isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_pickupShipmentReference_for_stops_TL_shipment_leg(args):
    """
    This function evaluates pickup Shipment Reference from Stops
    """

    qualifier_ = f"operations_custom.get_pickupShipmentReference_for_stops_TL"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating pickup Shipment Reference from Stops."
    logger.info(log_text, message_hdr_info)

    pickShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if pickShipmentLegID is not None:
        if isinstance(pickShipmentLegID, list):
            for id in pickShipmentLegID:
                if isinstance(ShipmentLegIDs, list):
                    for S_id in ShipmentLegIDs:
                        ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                        if id == ShipmentLegID:
                            output = {}
                            output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                            output['additionalShipmentId'] = [
                                {
                                    "typeCode": "SHIPPER_ASSIGNED",
                                    "value": "00000000000000000"
                                }
                            ]
                            output_list.append(output)
                            break
        return output_list
    else:
        return None


def get_dropoffShipmentReference_for_stops_TL_shipment_leg(args):
    """
    This function evaluates dropOff Shipment Reference from Stops
    """

    qualifier_ = f"operations_custom.get_dropoffShipmentReference_for_stops_TL"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dropOff Shipment Reference from Stops."
    logger.info(log_text, message_hdr_info)

    dropoffShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if dropoffShipmentLegID is not None:
        if isinstance(dropoffShipmentLegID, list):
            for id in dropoffShipmentLegID:
                if isinstance(ShipmentLegIDs, list):
                    for S_id in ShipmentLegIDs:
                        ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                        if id == ShipmentLegID:
                            output = {}
                            output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                            output['additionalShipmentId'] = [
                                {
                                    "typeCode": "SHIPPER_ASSIGNED",
                                    "value": "00000000000000000"
                                }
                            ]
                            output_list.append(output)
                            break
        return output_list
    else:
        return None


def get_entityId_transportReference_shipment_leg(args):
    """
    This function evaluates entityId for transport Reference
    """

    qualifier_ = f"operations_custom.get_entityId_transportReference"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating entityId for transport Reference."
    logger.info(log_text, message_hdr_info)

    loadDescription = args[0]
    referenceNumberStructure = args[1]
    if loadDescription is not None:
        return loadDescription
    elif isinstance(referenceNumberStructure, list):
        for ref_num_stuc in referenceNumberStructure:
            if hasattr(ref_num_stuc, 'referenceNumberTypeCode'):
                if getattr(ref_num_stuc, 'referenceNumberTypeCode') == 'PRO_ID':
                    return getattr(ref_num_stuc, 'referenceNumber')
    elif hasattr(referenceNumberStructure, 'referenceNumberTypeCode'):
        if getattr(referenceNumberStructure, 'referenceNumberTypeCode') == 'PRO_ID':
            return getattr(referenceNumberStructure, 'referenceNumber')

    return None


def get_total_pickup_stops_shipment_leg(args):
    """
    This function evaluates no. of pickup Stops
    """

    qualifier_ = f"operations_custom.get_total_pickup_stops"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating no. of pickup Stops."
    logger.info(log_text, message_hdr_info)

    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsPickedAtStop'):
                if getattr(stop, 'countOfShipmentsPickedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsPickedAtStop'):
            if getattr(stops, 'countOfShipmentsPickedAtStop') > 0:
                counter += 1
    return counter


def get_total_dropoff_stops_shipment_leg(args):
    """
    This function evaluates no. of dropOff Stops
    """

    qualifier_ = f"operations_custom.get_total_dropoff_stops"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating no. of dropOff Stops."
    logger.info(log_text, message_hdr_info)

    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsDroppedAtStop'):
                if getattr(stop, 'countOfShipmentsDroppedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsDroppedAtStop'):
            if getattr(stops, 'countOfShipmentsDroppedAtStop') > 0:
                counter += 1
    return counter


def get_dock_slot_identifier_shipment_leg(args):
    """
    This function evaluates dock slot identified(sublocationId) for Stop
    """

    qualifier_ = f"operations_custom.get_dock_slot_identifier"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock slot identified(sublocationId) for Stop."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockSlotIdentifier', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockSlotIdentifier', None)


def get_dockId_shipment_leg(args):
    """
    This function evaluates dock Id
    """

    qualifier_ = f"operations_custom.get_dockId"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock Id."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockCode', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockCode', None)


def get_dock_type_code_shipment_leg(args):
    """
    This function evaluates dock type code
    """

    qualifier_ = f"operations_custom.get_dock_type_code"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating dock type code."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'DockTypeEnumVal', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'DockTypeEnumVal', None)


def get_systemDockCommitmentID_shipment_leg(args):
    """
    This function evaluates system Dock CommitmentID
    """

    qualifier_ = f"operations_custom.get_systemDockCommitmentID"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating system Dock CommitmentID."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return False

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                if getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                    return getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID')

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            if getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                return getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID')

    return False


def get_document_action_code_transport_load_shipment_leg(args):
    """
    This function evaluates document Action Code
    """

    qualifier_ = f"operations_custom.get_document_action_code_transport_load"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating document Action Code."
    logger.info(log_text, message_hdr_info)

    start_status = args[0]
    event_name = args[1]
    current_load_status = args[2]
    visibility_options = args[3]
    message_header_info = args[-1]
    document_action_code = None
    list_ = visibility_options.get(start_status, {}).get('VisibilityOption', [])

    filtered_item = list(filter(lambda visibility_option: visibility_option.get(
                "CurrentLoadStatus") == current_load_status
                                                                  and visibility_option.get(
                "EventName") == event_name,
                                        list_))
    if len(filtered_item) > 0:
        document_action_code = filtered_item[0].get("DocumentOperationCode")

    return document_action_code


# Transport Load Custom Function - END

# TransporLoad Reference Number Update - START


def get_reference_number_for_load_tl_shipment_leg(args):
    """
    This function evaluates reference no for load TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference no for load TL reference update."
    logger.info(log_text, message_hdr_info)

    stops = args[0]
    eventName = args[1]
    referenceNumber = []
    if eventName in ['LoadTenderCancelled']:
        return None
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'shippingLocationCode'):
                referenceNumber.append(f"{str(getattr(stop, 'shippingLocationCode'))}-AWARE")

    else:
        if hasattr(stops, 'shippingLocationCode'):
            referenceNumber.append(f"{str(getattr(stops, 'shippingLocationCode'))}-AWARE")

    return ','.join(referenceNumber)


def get_reference_number_action_enum_for_load_tl_shipment_leg(args):
    """
    This function evaluates reference Number Action EnumVal for TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_action_enum_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference Number Action EnumVal for TL reference update."
    logger.info(log_text, message_hdr_info)

    eventName = args[0]
    if eventName == "LoadTenderAccepted":
        return "AT_ADD"
    elif eventName in ["LoadTenderUpdated", "transportLoadShipmentLegOperation"]:
        return "AT_UPDATE"
    elif eventName == "LoadTenderCancelled":
        return "AT_DELETE"
    return None


def get_system_reference_id_for_load_tl_shipment_leg(args):
    """
    This function evaluates reference Id for TL reference update
    """

    qualifier_ = f"operations_custom.get_system_reference_id_for_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference Id for TL reference update."
    logger.info(log_text, message_hdr_info)

    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled", "transportLoadShipmentLegOperation"]:
        return loadWHStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_system_reference_id_stop_load_tl_shipment_leg(args):
    """
    This function evaluates system reference Id for TL reference update
    """

    qualifier_ = f"operations_custom.get_system_reference_id_stop_load_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating system reference Id for TL reference update."
    logger.info(log_text, message_hdr_info)

    ReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(ReferenceNumberStructures, list):
        for RNS in ReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "systemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled", 'transportLoadShipmentLegOperation']:
        return stopStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_reference_number_for_stop_tl_shipment_leg(args):
    """
    This function evaluates reference number for stop for TL reference update
    """

    qualifier_ = f"operations_custom.get_reference_number_for_stop_tl"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating reference number for stop for TL reference update."
    logger.info(log_text, message_hdr_info)

    shippingLocationCode = args[0]
    eventName = args[1]
    if eventName in ['LoadTenderCancelled']:
        return None
    return shippingLocationCode + "-AWARE"


def get_shipment_transportReference_shipment_leg(args):
    """
    This function evaluates transportReference for shipment in TL
    """

    qualifier_ = f"operations_custom.get_shipment_transportReference"
    message_hdr_info = args[-1]

    log_text = f"{qualifier_} for evaluating transportReference for shipment in TL."
    logger.info(log_text, message_hdr_info)

    currentShipmentLeg = args[0]

    transportReference = []
    OrderConsolidationGroupID = get_value(currentShipmentLeg, "shipment.container.transportOrderInfo.orderConsolidationGroupID", None)

    if OrderConsolidationGroupID is not None:
        transportReference.append({
                "entityId": OrderConsolidationGroupID,
                "typeCode": 'ECN'
        })

    OrderNumberCode = get_value(currentShipmentLeg, "shipment.container.transportOrderInfo.orderNumberCode", None)
    ReferenceNumberStructure = get_value(currentShipmentLeg, "shipment.referenceNumberStructure")

    if isinstance(ReferenceNumberStructure, list):
        ReferenceNumber = None
        for ref_num in ReferenceNumberStructure:
            typecode = get_value(ref_num, "referenceNumberTypeCode")
            if typecode == "WM_ORDNUM_":
                ReferenceNumber = get_value(ref_num, "referenceNumber")

        if ReferenceNumber is not None and OrderNumberCode is None:
            transportReference.append({
                "entityId": ReferenceNumber,
                "typeCode": 'ON'
            })
    if len(transportReference)>0:
        return transportReference
    else:
        return None

# Shipment Leg Operation - Transport Load END

def get_value(dict_obj, attribute, default= None):
    item = dict_obj
    attr_list = attribute.split('.')

    if item is None:
        return default
    for attr in attr_list:
        if isinstance(item, list):
            item = item[0]
        if hasattr(item, attr):
            item = getattr(item, attr, None)
        else:
            return default
    return item
# TransporLoad Reference Number Update - END
