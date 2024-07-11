# start orders
def check_ship_from_address(*args):
    ref_struct = args[0][0]
    if ref_struct not in [None, '']:
        for ref in ref_struct:
            if eval('ref.referenceNumberValue') in ['P', 'EP']:
                return True
            else:
                return False
    return False


def check_ship_to_address(*args):
    ref_struct = args[0][0]
    if ref_struct not in [None, '']:
        for ref in ref_struct:
            if eval('ref.referenceNumberValue') not in ['P', 'EP']:
                return True
            else:
                return False
    return False


# end orders


# WON conditions
def check_valid_reference_number_update_for_won_load(args):
    transportLoadId = args[0]
    systemLoadId = args[1]
    loadReferenceId = args[2]
    if transportLoadId == systemLoadId:
        if systemLoadId not in ["", None] and loadReferenceId not in ["", None]:
            return True
    return False


def check_valid_reference_number_update_for_won_stop(args):
    transportLoadId = args[0]
    systemLoadId = args[1]
    stopReferenceId = args[2]
    if transportLoadId == systemLoadId:
        if systemLoadId not in ["", None] and stopReferenceId not in ["", None]:
            return True
    return False


# WON conditions end

def is_overrideday(calendar_type):
    return calendar_type[0].values[0] == "OverrideDays"


def is_regularweekshift(calendar_type):
    return calendar_type[0].values[0] == "RegularWeekshift"


def not_blank(*args):
    if args[0][0] is None or len(args[0][0]) == 0:
        return False
    elif len(args) == 0:
        return False
    return args[0][0]


# commenting to test the original scenario
# def check_calendar_id(value):
#     return value[0].values[0] not in [None, '']

def not_blank_check(args):
    if args[0] is None and args[1] is None and args[2] is None:
        return False
    return True


def is_blank(variable):
    if variable in [None, '']:
        return True
    return False


# Transport Load Custom Function - START

# Input: arguments as list
# desc: check if any of the argument is not null
# output: True or False
def check_if_any_arg_is_not_null(args):
    for arg in args:
        if arg is not None:
            return True
    return False


# Input: arguments as list
# desc: check if all the argument is null, else return True
# output: True or False
def check_if_all_arg_is_not_null(args):
    for arg in args:
        if arg is None:
            return False
    return True


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


def check_if_valid_picked_skids(args):
    pickedSkids = args[0]
    if pickedSkids is None:
        return False
    if pickedSkids == "":
        return False
    return True


def check_if_valid_dropped_skids(args):
    droppedSkids = args[0]
    if droppedSkids is None:
        return False
    if droppedSkids == "":
        return False
    return True


def check_if_valid_dock_commitment_id(args):
    shippingLocationCode = args[0]
    docks = args[1]
    if docks is None:
        return False

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                if getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                    return True

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            if getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                return True

    return False


# Transport Load Custom Function - END


# TransporLoad Reference Number Update - START


# Input: loadReferenceNumberStructures, eventName, loadWHStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or load warehouse status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_load_ref_update_tl(args):
    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "SystemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName == "LoadTenderUpdated":
        return True
    elif eventName == "LoadTenderAccepted":
        return True
    elif eventName == "LoadTenderCancelled":
        return True
    return False


# Input: stopReferenceNumberStructures, eventName, stopStatusRefNumber
# desc: return True is event Name is LoadTenderAccepted or stop status reference number
#       is present and event Name is LoadTenderUpdated
# output: True or False
def check_condition_for_stop_ref_update_tl(args):
    stopReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(stopReferenceNumberStructures, list):
        for RNS in stopReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "SystemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName == "LoadTenderUpdated":
        return True
    elif eventName == "LoadTenderAccepted":
        return True

    return False


# TransporLoad Reference Number Update - END


# Test Conditions

def greater_thn_11(variable):
    return variable > 11


def greater_thn_11_list(*args):
    return args[0][0] > 11


def not_blank_in_series(variable):
    if variable.values[0] is None:
        return False
    if variable.values[0] == '':
        return False
    return True


# TM Adapter Function

# Use Case:
#     Check if the shipment status is S_CONFIRMED or not
# Used In: Despatch Advice for process shipment container maintain API call
# Input: shipment Status(currentShipmentOperationalStatusEnumVal)
# Output: True if it's not equal false if it's equal.

def check_shipment_operational_status(args):
    if args != 'S_CONFIRMED':
        return True
    return False


def check_shipment_update_container_prerequiste(args):
    if args[0] in ['', None] or args[1] != 'true':
        return False
    return True


# Input: shipmentContainerDocument, property: override.planned.estimate
# output: Return True if override.planned.estimate is having True
#         value and shipmentContainerDocument is not Blank else False

def check_override_planned_true(argument_list):
    if argument_list[0] is None:
        return False
    return True if argument_list[1] in ['true', True] else False


# Input: shipmentContainerDocument, property: override.planned.estimate
# output: Return True if override.planned.estimate is having False
#         value and shipmentContainerDocument is not Blank else False

def check_override_planned_false(argument_list):
    if argument_list[0] is None:
        return False
    return False if argument_list[1] in ['true', True] else True


# Input: shipmentContainerDocument
# output: Return True shipmentContainerDocument is not Blank else False
def check_shipment_document(argument_list):
    return True if argument_list[0] is not None else False


# Input: referenceNumberActionEnumVal, systemLoadID, loadReferenceNumberID
# output: True or False
def check_condition_for_load_ref_update(argument_list):
    reference_number_action_enum_val = argument_list[0]
    system_load_id = argument_list[1]
    load_ref_id = argument_list[2]
    if system_load_id not in [None, ''] and (reference_number_action_enum_val == 'AT_UPDATE' or (
            reference_number_action_enum_val == 'AT_DELETE' and load_ref_id not in [None, ''])):
        return True
    return False


# Input: referenceNumberActionEnumVal, systemStopID, stopReferenceNumberID
# output: True or False
def check_condition_for_stop_ref_update(argument_list):
    reference_number_action_enum_val = argument_list[0]
    system_stop_id = argument_list[1]
    stop_ref_id = argument_list[2]
    if system_stop_id not in [None, ''] and (reference_number_action_enum_val == 'AT_UPDATE' or (
            reference_number_action_enum_val == 'AT_DELETE' and stop_ref_id not in [None, ''])):
        return True
    return False


# Input: stopDocument, current_da
# output: True or False

def check_condition_for_bol_ref_update(argument_list):
    stop_doc = argument_list[0]
    current_da = argument_list[1]
    try:
        for stop in eval('stop_doc.referenceNumberStructure'):
            if eval('stop.referenceNumberTypeCode') == 'OL' and eval('stop.systemReferenceNumberID') not in [None, ''] \
                    and eval('current_da.dropOffStop.billOfLadingNumber.entityId') not in [None, '']:
                return True

        return False
    except:
        return False


# Input: input_load_id,systemLoadID
# output: True or False
def check_new_and_old_load_id_not_same(argument_list):
    return True if argument_list[0] != argument_list[1] else False


def is_ild_summary_mandatory(args):
    if args:
        shipment_idl_flag = args[0][0].shipmentEntryModeEnumVal
        return shipment_idl_flag != "ILD_SUMMARY_MANDATORY"
    else:
        return False


""" Receiving Advice """


def check_shipment_leg(args):
    if args is not None and args != '':
        return True
    return False


def check_system_ref_no_id(argument_list):
    stop_ref_no_id = argument_list[0]
    if stop_ref_no_id not in [None, '']:
        return True
    return False


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
