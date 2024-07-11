from neo.log import Logger as logger
from types import SimpleNamespace as Namespace
import types
import json
from neo.process.transformation.utils.conversion_util import ConversionUtils

"""
forming payload based on event data and enrich data 
for mitcc flow
"""
def get_filtered_shipment_items_for_mitcc(*args):

    payload_shipment_details = args[0][0] if args[0][0] else []
    enrich_shipment_details = args[0][1] if args[0][1] else []

    if not isinstance(payload_shipment_details, list):
        payload_shipment_details = [payload_shipment_details]

    size_of_shipment = len(payload_shipment_details)

    parentWMShipmentId = get_value(payload_shipment_details[0], 'shipmentItem.parentShipmentId')

    result_data = []

    if size_of_shipment == 1:
        for current_shipment in payload_shipment_details:
            wmShipmentIdentification = get_value(current_shipment, 'shipmentId.primaryId')

            # loop on shipment item
            current_shipment_items = get_value(current_shipment, 'shipmentItem')

            if not isinstance(current_shipment_items, list):
                current_shipment_items = [current_shipment_items]

            hash_tms_shipment_id_visited = []
            for distinct_item in current_shipment_items:

                transactionalReferences = get_value(distinct_item,'transactionalReference', [])

                tmShipmentId = [getattr(t, "entityId") for t in transactionalReferences
                                if getattr(t, 'transactionalReferenceTypeCode') == "SRN"
                                and wmShipmentIdentification != get_value(distinct_item, "parentShipmentId")
                                and get_value(distinct_item, 'plannedDespatchQuantity.value', 0) != 0]

                if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                    continue
                else:
                    hash_tms_shipment_id_visited.append(tmShipmentId)

                if len(tmShipmentId) > 0:

                    ShipmentDocument_currentShipment = [shipment_data for shipment_data in enrich_shipment_details
                                                        if get_value(shipment_data, 'shipmentNumber') == tmShipmentId[0]
                                                        and get_value(shipment_data, 'shipmentNumber') not in [None, ""]]

                    if ShipmentDocument_currentShipment:
                        ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]
                    else:
                        ShipmentDocument_currentShipment = None

                    if get_value(ShipmentDocument_currentShipment,
                                 'mergeInTransitConsolidationCode') not in [None, '']:
                        result_data.append(Namespace(**{
                            "currentShipment": ShipmentDocument_currentShipment,
                            "currentEntity": current_shipment,
                            "tmShipmentId": tmShipmentId,
                        }))

    elif size_of_shipment == 2:
        for current_shipment in payload_shipment_details:
            wmShipmentIdentification = get_value(current_shipment, 'shipmentId.primaryId')

            if wmShipmentIdentification != parentWMShipmentId:
                isUpdateshipment = False
            else:
                isUpdateshipment = True

            # loop on shipment item
            current_shipment_items = get_value(current_shipment, 'shipmentItem')

            if not isinstance(current_shipment_items, list):
                current_shipment_items = [current_shipment_items]

            hash_tms_shipment_id_visited = []
            for distinct_item in current_shipment_items:

                transactionalReferences = get_value(distinct_item, 'transactionalReference', [])

                tmShipmentId = [getattr(t, "entityId") for t in transactionalReferences
                                if getattr(t, 'transactionalReferenceTypeCode') == "SRN"
                                and wmShipmentIdentification != get_value(distinct_item, "parentShipmentId")
                                and get_value(distinct_item, 'plannedDespatchQuantity.value', 0) != 0]

                if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                    continue
                else:
                    hash_tms_shipment_id_visited.append(tmShipmentId)

                if len(tmShipmentId) > 0:

                    ContainersInWOI = []
                    for shipment in payload_shipment_details:
                        if get_value(shipment, 'shipmentId.primaryId') == parentWMShipmentId:
                            for shipmentItem in get_value(shipment, "shipmentItem", []):
                                for tR in get_value(shipmentItem, "transactionalReference"):
                                    if getattr(tR, 'transactionalReferenceTypeCode') == "SRN" and get_value(tR,
                                                                                                            'entityId') == tmShipmentId[0]:
                                        ContainersInWOI.append(shipmentItem)
                                        break

                    sizeOfContainersInWOI = len(ContainersInWOI)

                    if isUpdateshipment == False and sizeOfContainersInWOI == 0:

                        ShipmentDocument_currentShipment = [shipment_data for shipment_data in enrich_shipment_details
                                                            if get_value(shipment_data, 'shipmentNumber') == tmShipmentId[0]
                                                            and get_value(shipment_data, 'shipmentNumber') not in [None,
                                                                                                                   ""]]

                        if ShipmentDocument_currentShipment:
                            ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]
                        else:
                            ShipmentDocument_currentShipment = None

                        if get_value(ShipmentDocument_currentShipment,
                                     'mergeInTransitConsolidationCode') not in [None, '']:
                            result_data.append(Namespace(**{
                                "currentShipment": ShipmentDocument_currentShipment,
                                "currentEntity": current_shipment,
                                "tmShipmentId": tmShipmentId,
                            }))

    if len(result_data) == 0:
        return None
    return result_data


"""
forming container id data based on event data and enrich data 
for container update flow
"""
def get_system_container_id(*args):

    try:
        payload_shipment_details = args[0][0] if args[0][0] else []
        enrich_shipment_details = args[0][1] if args[0][1] else []
        message_hdr_info = args[0][2]

        if not isinstance(payload_shipment_details, list):
            payload_shipment_details = [payload_shipment_details]

        result = []
        for current_shipment in payload_shipment_details:
            wmShipmentIdentification = get_value(current_shipment, 'shipmentId.primaryId')

            # loop on shipment item
            current_shipment_items = get_value(current_shipment, 'shipmentItem', [])

            if not isinstance(current_shipment_items, list):
                current_shipment_items = [current_shipment_items]

            hash_tms_shipment_id_visited = []
            for distinct_item in current_shipment_items:
                shipment_container = []
                transactionalReferences = get_value(distinct_item, 'transactionalReference', [])

                tmShipmentId = [getattr(t, 'entityId') for t in transactionalReferences
                                if getattr(t,'transactionalReferenceTypeCode') == "SRN" and
                                wmShipmentIdentification == get_value(distinct_item, 'parentShipmentId')]

                if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                    continue
                else:
                    hash_tms_shipment_id_visited.append(tmShipmentId)

                if len(tmShipmentId) > 0:

                    ShipmentDocument_currentShipment = [shipment_data for shipment_data in enrich_shipment_details
                                                        if getattr(shipment_data, 'shipmentNumber') == tmShipmentId[0]
                                                        and getattr(shipment_data, 'shipmentNumber') not in [None, ""]]

                    if ShipmentDocument_currentShipment:
                        ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]
                    else:
                        ShipmentDocument_currentShipment = None


                    for curr_container in get_value(ShipmentDocument_currentShipment, 'container', []):

                        containerLineItemNumber = getattr(curr_container, 'systemContainerID')
                        lineItemNumber = None
                        for shipmentItem in current_shipment_items:
                            for transactionalReference in get_value(shipmentItem, 'transactionalReference', []):
                                if getattr(transactionalReference, 'transactionalReferenceTypeCode') == "SRN":

                                    if getattr(transactionalReference, 'lineItemNumber') == int(containerLineItemNumber) and \
                                            getattr(transactionalReference, 'entityId') == tmShipmentId[0]:

                                        lineItemNumber = getattr(transactionalReference, 'lineItemNumber')

                        if lineItemNumber in [None, ""]:
                            shipment_container.append({'containerLineItemNumber': containerLineItemNumber})

                    serialised_message = json.dumps(shipment_container)
                    deserialised_message = json.loads(serialised_message,
                                                      object_hook=lambda d: types.SimpleNamespace(**d))
                    result.append(Namespace(**{
                        'containers': deserialised_message
                    }))

        return result

    except Exception as e:
        logger.error(f'Exception errors order cancel flow {e}', message_hdr_info)
        return None



"""
forming shipment number data based on event data and enrich data 
for shipment delete flow
"""
def get_shipment_number(*args):

    distinct_items = args[0][0] if args[0][0] else []

    for entity in get_value(distinct_items, 'transactionalReference', []):
        if get_value(entity, 'transactionalReferenceTypeCode') == 'SRN':
            return get_value(entity, 'entityId')



"""
forming container id data based on event data and enrich data 
for update container flow
"""
def get_filtered_shipment_items(args, original_payload, global_variable):
    def if_valid_container(currentContainer, currentItem):
        ReferenceNumberStructure = get_value(currentContainer, "referenceNumberStructure", [])
        for ref in ReferenceNumberStructure:
            if getattr(ref, "referenceNumberTypeCode", None) == "WM_ORDLIN_" and int(getattr(ref, "referenceNumber", -100)) == (get_value(currentItem, "transactionalReference.lineItemNumber",-1)):
                return True
        return False

    resulted_items = []
    event_shipments = args[0]
    enriched_tms_shipments = args[1]

    if not isinstance(event_shipments, list):
        event_shipments = [event_shipments]

    for current_shipment in event_shipments:
        wmShipmentIdentification = get_value(current_shipment, "shipmentId.primaryId")

        #loop on shipment item
        current_shipment_items = get_value(current_shipment, "shipmentItem")

        if not isinstance(current_shipment_items, list):
            current_shipment_items = [current_shipment_items]

        hash_tms_shipment_id_visited = []

        for distinct_item in current_shipment_items:
            shipment_container = []
            transactionalReferences = get_value(distinct_item, "transactionalReference")
            tmShipmentId = [ getattr(t, "entityId") for t in transactionalReferences if getattr(t, 'transactionalReferenceTypeCode') == "SRN" and wmShipmentIdentification == get_value(distinct_item, "parentShipmentId")]
            if tmShipmentId:
                tmShipmentId = tmShipmentId[0]
            else:
                tmShipmentId = None

            if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                continue
            else:
                hash_tms_shipment_id_visited.append(tmShipmentId)

            ShipmentDocument_currentShipment = [ shipment for shipment in enriched_tms_shipments
                                  if getattr(shipment, "shipmentNumber") == tmShipmentId
                                  and getattr(shipment, "shipmentNumber") not in [None, ""] ]

            if ShipmentDocument_currentShipment:
                ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]
            else:
                ShipmentDocument_currentShipment = None

            if ShipmentDocument_currentShipment not in [None, ""]:
                for current_item in current_shipment_items:
                    transactionalReferences_ = get_value(current_item, "transactionalReference")
                    transactionalReference_entityId = [getattr(t, "entityId") for t in transactionalReferences_ if
                                    getattr(t, 'transactionalReferenceTypeCode') == "SRN"]
                    if transactionalReference_entityId:
                        transactionalReference_entityId = transactionalReference_entityId[0]
                    else:
                        continue
                    if transactionalReference_entityId == tmShipmentId and get_value(current_item, "plannedDespatchQuantity.value") != "0" :
                        shipmentContainerDocument = [
                            currentContainer
                            for currentContainer in get_value(ShipmentDocument_currentShipment, "container", [])
                            if if_valid_container(currentContainer, current_item)
                        ]

                        shipmentContainerDocument[0].__dict__.update(current_item.__dict__)
                        shipmentContainerDocument[0].__dict__.update({'tmShipmentId': tmShipmentId,
                                                                      'shipmentEntryModeEnumVal':
                                                                          get_value(ShipmentDocument_currentShipment,
                                                                                    "shipmentEntryModeEnumVal", None),
                                                                      'splitMethodEnumVal': get_value(
                                                                          ShipmentDocument_currentShipment,
                                                                          "splitMethodEnumVal", None)
                                                                      })

                        shipment_container.append(shipmentContainerDocument[0])

            resulted_items.append(Namespace(**{
                "currentContainer": shipment_container
            }))
    return resulted_items


"""
forming shipment data based on event data and enrich data 
for shipment create flow
"""
def get_filtered_shipment_items_add(args, original_payload, global_variable):
    resulted_items = []
    event_shipments = args[0]
    enriched_tms_shipments = args[1]

    if not isinstance(event_shipments, list):
        event_shipments = [event_shipments]

    for current_shipment in event_shipments:
        wmShipmentIdentification = get_value(current_shipment, "shipmentId.primaryId")

        #loop on shipment item
        current_shipment_items = get_value(current_shipment, "shipmentItem")

        if not isinstance(current_shipment_items, list):
            current_shipment_items = [current_shipment_items]

        hash_tms_shipment_id_visited = []

        for distinct_item in current_shipment_items:
            transactionalReferences = get_value(distinct_item, "transactionalReference")
            tmShipmentId = [ getattr(t, "entityId") for t in transactionalReferences
                             if getattr(t, 'transactionalReferenceTypeCode') == "SRN"
                             and wmShipmentIdentification != get_value(distinct_item, "parentShipmentId")
                             and get_value(distinct_item, 'plannedDespatchQuantity.value', 0) != 0]
            if tmShipmentId:
                tmShipmentId = tmShipmentId[0]
            else:
                tmShipmentId = None

            if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                continue
            else:
                hash_tms_shipment_id_visited.append(tmShipmentId)

            ShipmentDocument_currentShipment = [ shipment for shipment in enriched_tms_shipments
                                  if getattr(shipment, "shipmentNumber") == tmShipmentId
                                  and getattr(shipment, "shipmentNumber") not in [None, ""] ]

            if ShipmentDocument_currentShipment:
                ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]
            else:
                ShipmentDocument_currentShipment = None

            resulted_items.append(Namespace(**{
                "currentShipment": ShipmentDocument_currentShipment,
                "currentEntity": current_shipment,
                "tmShipmentId": tmShipmentId,
                "currentItem": distinct_item,
                "wmShipmentIdentification": wmShipmentIdentification
            }))

    return resulted_items


"""
forming container data based on event data and enrich data 
for shipment create flow
"""
def get_filtered_shipment_items_containers(args, original_payload, global_variable):
    def if_valid_container(currentContainer, currentItem):
        ReferenceNumberStructure = get_value(currentContainer, "referenceNumberStructure", [])
        for ref in ReferenceNumberStructure:
            if getattr(ref, "referenceNumberTypeCode", None) == "WM_ORDLIN_" and int(getattr(ref, "referenceNumber", -100)) == (get_value(currentItem, "transactionalReference.lineItemNumber",-1)):
                return True
        return False

    currentEntity = args[0]
    currentShipment = args[1]
    tmShipmentId = args[2]
    current_shipment_items = get_value(currentEntity, "shipmentItem")
    resulted_items = []
    if not isinstance(current_shipment_items, list):
        current_shipment_items = [current_shipment_items]

    for currentItem in current_shipment_items:
        transactionalReferences_ = get_value(currentItem, "transactionalReference")
        transactionalReference_entityId = [getattr(t, "entityId") for t in transactionalReferences_ if
                                           getattr(t, 'transactionalReferenceTypeCode') == "SRN"]
        if transactionalReference_entityId:
            transactionalReference_entityId = transactionalReference_entityId[0]
        else:
            continue
        if transactionalReference_entityId == tmShipmentId and get_value(currentShipment,
                                                                         "plannedDespatchQuantity.value") != 0:
            shipmentContainerDocument = [
                currentContainer
                for currentContainer in get_value(currentShipment, "container", [])
                if if_valid_container(currentContainer, currentItem)
            ]

            inputFreightClassCode = get_value(currentItem, "freightClassCode")

            resulted_items.append(Namespace(**{
                "currentShipment": currentShipment,
                "currentEntity": currentEntity,
                "tmShipmentId": tmShipmentId,
                "currentItem": currentItem,
                "currentContainer": shipmentContainerDocument,
                "inputFreightClassCode": inputFreightClassCode
            }))

    return resulted_items


"""
forming current shipment data based on event data and enrich data 
for remove shipment from current load flow
"""
def get_remove_shipment_from_current_load(args, original_payload, global_variable):
    event_shipments = args[0]
    enriched_tms_shipments = args[1]

    if not isinstance(event_shipments, list):
        event_shipments = [event_shipments]

    sizeOfShipment = len(event_shipments)

    result_removeFromPlannedLoad = []

    if sizeOfShipment == 1:
        for current_shipment in event_shipments:
            wmShipmentIdentification = get_value(current_shipment, "shipmentId.primaryId")

            # loop on shipment item
            current_shipment_items = get_value(current_shipment, "shipmentItem")

            if not isinstance(current_shipment_items, list):
                current_shipment_items = [current_shipment_items]

            hash_tms_shipment_id_visited = []

            for distinct_item in current_shipment_items:
                transactionalReferences = get_value(distinct_item, "transactionalReference")

                tmShipmentId = [getattr(t, "entityId") for t in transactionalReferences
                                if getattr(t, 'transactionalReferenceTypeCode') == "SRN"
                                and wmShipmentIdentification != get_value(distinct_item, "parentShipmentId")
                                and get_value(distinct_item, 'plannedDespatchQuantity.value', 0) != 0]

                if tmShipmentId:
                    tmShipmentId = tmShipmentId[0]
                else:
                    tmShipmentId = None

                if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                    continue
                else:
                    hash_tms_shipment_id_visited.append(tmShipmentId)

                ShipmentDocument_currentShipment = [shipment for shipment in enriched_tms_shipments
                                                    if getattr(shipment, "shipmentNumber") == tmShipmentId
                                                    and getattr(shipment, "shipmentNumber") not in [None, ""]]

                if ShipmentDocument_currentShipment:
                    ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]

                result_removeFromPlannedLoad.append(Namespace(**{
                "currentShipment": ShipmentDocument_currentShipment,
                "distinctItem": distinct_item
                }))

    elif sizeOfShipment == 2:
        for current_shipment in event_shipments:
            wmShipmentIdentification = get_value(current_shipment, "shipmentId.primaryId")
            parentWMShipmentId = [get_value(shipmentItem, "parentShipmentId") for shipment in event_shipments
                                  for shipmentItem in get_value(shipment, "shipmentItem", [])]

            if isinstance(parentWMShipmentId, list):
                parentWMShipmentId = parentWMShipmentId[0]

            isUpdateshipment = False if wmShipmentIdentification != parentWMShipmentId else True

            # loop on shipment item
            current_shipment_items = get_value(current_shipment, "shipmentItem")

            if not isinstance(current_shipment_items, list):
                current_shipment_items = [current_shipment_items]

            hash_tms_shipment_id_visited = []

            for distinct_item in current_shipment_items:
                transactionalReferences = get_value(distinct_item, "transactionalReference")
                tmShipmentId = [getattr(t, "entityId") for t in transactionalReferences
                                if getattr(t, 'transactionalReferenceTypeCode') == "SRN"
                                and wmShipmentIdentification != get_value(distinct_item, "parentShipmentId")
                                and get_value(distinct_item, 'plannedDespatchQuantity.value', 0) != 0]
                if tmShipmentId:
                    tmShipmentId = tmShipmentId[0]
                else:
                    tmShipmentId = None

                if tmShipmentId in hash_tms_shipment_id_visited or tmShipmentId is None:
                    continue
                else:
                    hash_tms_shipment_id_visited.append(tmShipmentId)

                ContainersInWOI = []
                for shipment in event_shipments:
                    if get_value(shipment, 'shipmentId.primaryId') == parentWMShipmentId:
                        for shipmentItem in get_value(shipment, "shipmentItem", []):
                            for tR in get_value(shipmentItem, "transactionalReference"):
                                if getattr(tR, 'transactionalReferenceTypeCode') == "SRN" and get_value(tR, 'entityId') == tmShipmentId:
                                    ContainersInWOI.append(shipmentItem)
                                    break


                sizeOfContainersInWOI = len(ContainersInWOI)

                ShipmentDocument_currentShipment = [shipment for shipment in enriched_tms_shipments
                                                    if getattr(shipment, "shipmentNumber") == tmShipmentId
                                                    and getattr(shipment, "shipmentNumber") not in [None, ""]]

                if ShipmentDocument_currentShipment:
                    ShipmentDocument_currentShipment = ShipmentDocument_currentShipment[0]

                if sizeOfContainersInWOI == 0 and not isUpdateshipment:
                    result_removeFromPlannedLoad.append(Namespace(**{
                        "currentShipment": ShipmentDocument_currentShipment,
                        "distinctItem": distinct_item
                    }))

    return result_removeFromPlannedLoad



"""
forming reference number structure data based on reference number
and wm shipment identification for shipment add flow
"""
def get_shipment_reference_number_structure(arguments_list):

    reference_numbers = arguments_list[0]
    wmshipment_identification = arguments_list[1]

    result_reference_number = []
    if reference_numbers is not None:

        dict_reference_number = namespace_to_dict([reference_numbers])

        if not isinstance(dict_reference_number, list):
            dict_reference_number = [dict_reference_number]

        result_reference_number.extend(dict_reference_number)

    result_reference_number.append(
        {
            'referenceNumberTypeCode': 'WM_SPLIT_SHP',
            'referenceNumber': wmshipment_identification
        }
    )

    return result_reference_number




"""
flatten the dict obj to return value of a given field
"""
def get_value(dict_obj, attribute, default=None):
    item = dict_obj
    attr_list = attribute.split('.')

    if item is None:
        return default
    for attr in attr_list:
        if isinstance(item, list) and len(item)>0:
            item = item[0]
        if hasattr(item, attr):
            item = getattr(item, attr, None)
        else:
            return default
    return item


"""
form freight class code based on arguments current item and 
current container frieght class code 
"""
def get_freight_class_code(argument_list):

    input_frieght_class_code = argument_list[0]
    curr_container_frieght_class_code = argument_list[1]

    if curr_container_frieght_class_code is not None:
        return curr_container_frieght_class_code
    else:
        return "*FAK"


"""
Return uom converted value for weight, volume and height
"""
def get_uom_conversion(argument_list):

    value = argument_list[0]
    source_uom = argument_list[1]
    qty = argument_list[2]

    target_uom = argument_list[3]
    uom_type = argument_list[4]

    converted_value = ConversionUtils.getUomConversion(uom_type, source_uom, target_uom,
                                                         value)

    if converted_value is not None and qty is not None:

        converted_value = float(converted_value)
        qty = float(qty)

        return format(converted_value/qty, '.4f')



"""
return shipment leg id from current shipment
"""
def get_system_shipment_leg_id(argument_list):
    shipmentLeg = argument_list[0]

    if not isinstance(shipmentLeg, list):
        shipmentLeg = [shipmentLeg]

    result = []
    for leg in shipmentLeg:
        leg_id = get_value(leg, 'systemShipmentLegID')

        if leg_id is not None:
            result.append(leg_id)

    return result


def namespace_to_dict(args):
    """
    This function converts the namespace to dict to make it json serializable
    """

    def list_of_ns(arg: list):
        """
        assumption: list will have elems of same type and the check would have done for the first elem before calling the func()
        1. Iterate the list for all elems
        2. Call simple_ns for each elem
        3. Return once iterated all the elems
        """
        updated_arg = []
        for elem in arg:
            updated_arg.append(simple_ns(elem))
        return updated_arg

    def simple_ns(arg: Namespace):
        """
        1. Convert the SimpleNamespace to dict
        2. Iterate to check if a key is an instance of SimpleNamespace or if a list is present
            a. if instance of SimpleNamespace, call the simple_ns()
            b. if instance of list and list[0] is an instance of SimpleNamespace; call list_of_ns()
            c. continue
        3. Return from the function when iterated all the keys of the dict
        """
        arg = vars(arg)
        for item in arg.keys():
            if isinstance(arg[item], Namespace):
                arg[item] = simple_ns(arg[item])
            elif isinstance(arg[item], list) and isinstance(arg[item][0], Namespace):
                arg[item] = list_of_ns(arg[item])
            else:
                continue

        return arg

    args = args[0]
    if isinstance(args, list) and isinstance(args[0], Namespace):
        args = list_of_ns(args)
    if isinstance(args, Namespace):
        args = simple_ns(args)

    return args


"""
Function to get note details by 
converting namespace to dict
"""
def get_note_details(argument_list):

    note_val = argument_list[0]
    return namespace_to_dict([note_val])

