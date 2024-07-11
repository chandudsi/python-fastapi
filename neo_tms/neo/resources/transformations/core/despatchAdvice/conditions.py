"""
Input: referenceNumberActionEnumVal, systemLoadID, loadReferenceNumberID
output: True or False
"""
def check_condition_for_load_ref_update(argument_list):
    reference_number_action_enum_val = argument_list[0]
    system_load_id = argument_list[1]
    load_ref_id = argument_list[2]
    if system_load_id not in [None, ''] and (reference_number_action_enum_val == 'AT_UPDATE' or (reference_number_action_enum_val == 'AT_DELETE' and load_ref_id not in [None, ''])):
        return True
    return False



"""
Input: referenceNumberActionEnumVal, systemStopID, stopReferenceNumberID
output: True or False
"""
def check_condition_for_stop_ref_update(argument_list):
    reference_number_action_enum_val = argument_list[0]
    system_stop_id = argument_list[1]
    stop_ref_id = argument_list[2]
    if system_stop_id not in [None, ''] and (reference_number_action_enum_val == 'AT_UPDATE' or (
            reference_number_action_enum_val == 'AT_DELETE' and stop_ref_id not in [None, ''])):
        return True
    return False



"""
Input: stopDocument, current_da
output: True or False
"""
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
    
def is_lineItem_processable(*args):
    ext_shipmentContainerDocument, \
    ext_shipmentToProcess, \
    ext_shipmentOperationStatus, \
    default_shipmentOperationStatus, \
    documentActionCode, not_allowed_action_code, *_= args[0]

    return ext_shipmentContainerDocument == True and \
        ext_shipmentToProcess == True and \
        ext_shipmentOperationStatus != default_shipmentOperationStatus and \
        documentActionCode != not_allowed_action_code
