import neo.process.transformation.utils.operation_util as operation_utils
import copy
from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger

look_up_dict = adapter_cps_provider.get_properties().get("codMapConfig")

"""##############################OrderRelease functions start ##########################################"""


def get_remove_shipment_details(*args):
    """
    This function is used for getting the list of shipment which needs to be removed from planned load before
    cancelling/deleting a shipment for order Release.
    :param args: list of a tuple of item(order Release, shipment enrichment data)
    :return: list of shipment which needs to be deleted
    """
    orderRelease = args[0][0] if args[0][0] else []
    shipment_details = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]
    shipment_list = set()
    if getattr(orderRelease[0], "documentActionCode", None) in ["CANCEL", "CHANGE_BY_REFRESH", "DELETE"]:
        for shipment in shipment_details:
            if getattr(shipment, 'shipmentNumber') == getattr(orderRelease[0], 'orderId'):
                for shipmentLegId in getattr(shipment, "shipmentLeg", []):
                    shipment_list.add(getattr(shipmentLegId, 'systemShipmentLegID'))
    logger.debug(f"List of shipment which needs to be removed from the planned load is: {shipment_list}",
                 message_hdr_info)
    return list(shipment_list) if len(shipment_list) else None


def get_shipment_details(*args):
    """
    This function is used for getting the correct enriched shipment details for a given orderId for order Release.
    :param args: list of a tuple of item(orderId, shipments enrichment data, message hdr info)
    :return: enriched shipment details from the shipments enrichment object
    """
    orderId = args[0][0]
    shipment_details = args[0][1] if args[0][1] else []
    if shipment_details is None:
        shipment_details = []
    for shipment in shipment_details:
        if eval('shipment.shipmentNumber') == orderId:
            return shipment


def get_customer_details(*args):
    """
    This function is used for getting the correct enriched customer details for a given customerCode for order Release.
    :param args: list of a tuple of item(customer, customer enrichment data, message hdr info)
    :return: enriched customer details from the customer enrichment object
    """
    customerCode = args[0][0]
    customer_details = args[0][1] if args[0][1] else []
    for customer in customer_details:
        if eval('customer.customerCode') == customerCode:
            return customer


def get_shippingLocation_details(*args):
    """
    This function is used for getting the correct enriched shippingLocation details for a given shippingLocationCode
     for order Release.
    :param args: list of a tuple of item(shippingLocationCode, shippingLocation enrichment data, message hdr info)
    :return: enriched shippingLocation details from the shippingLocation enrichment object
    """
    shippingLocationCode = args[0][0]
    shippingLocationCode_details = args[0][1] if args[0][1] else []
    for shippingLocation in shippingLocationCode_details:
        if eval('shippingLocation.shippingLocationCode') == shippingLocationCode:
            return shippingLocation


def get_active_shipment_lines(*args):
    """
    This function is used for getting the active lines for a given order, so it will be required for shipment update
     for order Release.
    :param args: list of a tuple of item(line_details, message hdr info)
    :return: list of active lines for a given order
    """
    line_details = args[0][0] if args[0][0] else []
    active_lines = []
    for line in line_details:
        # if getattr(line, "isOrderLineAllocatable", True) is False:
        #     continue
        if getattr(line, "actionCode", None) == "CANCEL":
            continue
        active_lines.append(line)
    return active_lines


def get_division_code(*args):
    """
    This function is used for calculating the division code when a shipment create api call will be executed
     for order Release.
    :param args: list of a tuple of item(documentActionCode, organisation_name, ship_from_code,message hdr info)
    :return: division_code
    """
    ship_from_code = args[0][2]
    organisation_name = args[0][1]
    message_hdr_info = args[0][3]
    ship_from_warehouse_look_up = look_up_dict.get("warehouses", {}).get(ship_from_code, {})
    if args[0][0] == "ADD":
        if organisation_name:
            division_code = organisation_name
        else:
            division_code = ship_from_warehouse_look_up.get("DivisionCode", None)
    else:
        division_code = None
    logger.debug(f"Calculated division code is : {division_code}", message_hdr_info)
    return division_code


def get_logisticsGroup_code(*args):
    """
    This function is used for calculating the logistics_group_code
     for order Release.
    :param args: list of a tuple of item(documentActionCode, buyer, ship_from_code,message hdr info)
    :return: logistics_group_code
    """
    logistics_group_code = None
    buyer = args[0][1]
    ship_from_warehouse_look_up = look_up_dict.get("warehouses", {}).get(args[0][2], {})
    message_hdr_info = args[0][3]
    if args[0][0] == "ADD" and buyer:
        if len(getattr(buyer, "contact", [])):
            logistics_group_code = getattr(getattr(buyer, "contact")[0], "contactTypeCode")
        if logistics_group_code == "CZL":
            logistics_group_code = getattr(getattr(buyer, "contact")[0], "departmentName")
        else:
            logistics_group_code = ship_from_warehouse_look_up.get("LogisticsGroupCode")

    logger.debug(f"Calculated division code is : {logistics_group_code}", message_hdr_info)
    return logistics_group_code


def get_shipmentEntryType_id(*args):
    """
     This function is used for calculating the shipment split method
     for order Release.
     :param args: list of a tuple of item(line_details, split method ,message hdr info)
     :return: split_method
    """
    line_details = args[0][0] if args[0][0] else []
    split_method = None
    message_hdr_info = args[0][2]
    if len(list(filter(lambda line: getattr(line, "isOrderLineAllocatable", None) is False, line_details))):
        split_method = "SPLIT_METHOD_NONE"
    elif args[0][1]:
        split_method = look_up_dict.get("shipmentSplitMethodCodeFromGS1ToApp", {}). \
            get(args[0][1])
    logger.debug(f"Calculated division code is : {split_method}", message_hdr_info)
    return split_method


def get_shipment_priority(*args):
    """
     This function is used for calculating the shipment priority for order Release.
     :param args: list of a tuple of item(line_details, order_priority ,message hdr info)
     :return: split_method
    """
    line_details = args[0][0] if args[0][0] else []
    order_priority = args[0][1]
    message_hdr_info = args[0][2]
    shipment_priority_aggregation_method = look_up_dict.get("shipmentLinePriority.aggregationMethod")
    priority = None
    if order_priority:
        priority = order_priority
    else:
        priority_list = []
        for line in line_details:
            priority_list.append(int(getattr(line, "orderLinePriority", 0)))

        if shipment_priority_aggregation_method == "HIGHEST":
            priority = sorted(priority_list)[0]
        elif shipment_priority_aggregation_method == "HIGHEST":
            priority = sorted(priority_list, reverse=True)[0]
        else:
            priority = sum(priority_list) // len(priority_list)
    logger.debug(f"Calculated shipment priority code is : {priority}", message_hdr_info)
    return priority


def concat_values(*args):
    """
     This function is used for adding to string and if second string is not none then get the element from the 3rd
     to last, it is being used for calculating the shipFromLocationTypeEnumVal and shipToLocationTypeEnumVal
     for order Release.
     :param args: list of a tuple of item(constant(SFT/STT), shippingLocationTypeEnumVal ,message hdr info)
     :return: concatenated value.
    """
    return args[0][0] + args[0][1][3:] if len(args[0]) > 1 and args[0][1] is not None else args[0][0]


def time_stamp_offset_check_tms(time):
    """
     This function is used for checking that in the given time is there a timezone offset is there or not
     for order Release.
     :param args: list of a tuple of item(time)
     :return: True if time is having timezone info else false
    """
    return True if '+' in time or '-' in time else False


def set_pick_up_dropOff_dates(*args):
    """
     This function is used for calculating the pickup to/from and dropoff to/from dates
     for order Release.
     :param args: list of a tuple of item(order logistical information, ship_from_location enrichment details,
     ship_to_location enrichment details, message_hdr_info)
     :return: dictionary of the calculated dates
    """
    ship_from_location_businessHours = getattr(args[0][1], "businessHours")
    ship_to_location_businessHours = getattr(args[0][2], "businessHours")
    order_logistical_information = args[0][0]
    message_hdr_info = args[0][3]
    expected_result_dict = {}
    delivery_dates_info = getattr(order_logistical_information, 'requestedDeliveryDateRange', None)
    ship_dates_info = getattr(order_logistical_information, 'requestedShipDateRange', None)
    if delivery_dates_info and getattr(delivery_dates_info, 'beginDate'):
        logger.debug(f'preparing the delivery from and to date time', message_hdr_info)
        if time_stamp_offset_check_tms(getattr(delivery_dates_info, 'beginTime')):
            delivery_from_date_time = operation_utils.OperationUtils.convertGs1ToLocationDate(getattr(delivery_dates_info, 'beginDate'),
                                                                              getattr(delivery_dates_info, 'beginTime'),
                                                                              getattr(ship_to_location_businessHours,
                                                                                      'timeZoneOffset'),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults"
                                                                                  ".timeZone"),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults."
                                                                                  "tmDateFormat"),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults"
                                                                                  ".gS1DateFormat")
                                                                              )

            delivery_to_date_time = operation_utils.OperationUtils.convertGs1ToLocationDate(getattr(delivery_dates_info, 'endDate'),
                                                                            getattr(delivery_dates_info, 'endTime'),
                                                                            getattr(ship_to_location_businessHours,
                                                                                    'timeZoneOffset'),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults"
                                                                                ".timeZone"),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults."
                                                                                "tmDateFormat"),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults"
                                                                                ".gS1DateFormat")
                                                                            )

        else:
            delivery_from_date_time = getattr(delivery_dates_info, 'beginDate') + 'T' + getattr(delivery_dates_info,
                                                                                                'beginTime')
            delivery_to_date_time = getattr(delivery_dates_info, 'endDate') + 'T' + getattr(delivery_dates_info,
                                                                                            'endTime')

        expected_result_dict.update({'deliveryFromDateTime': delivery_from_date_time,
                                     'deliveryToDateTime': delivery_to_date_time})

    if ship_dates_info and getattr(ship_dates_info, 'beginDate'):
        logger.info(f'preparing the pick from and to date time', message_hdr_info)
        if time_stamp_offset_check_tms(getattr(ship_dates_info, 'beginTime')):
            pickup_from_dates_times = operation_utils.OperationUtils.convertGs1ToLocationDate(getattr(ship_dates_info, 'beginDate'),
                                                                              getattr(ship_dates_info, 'beginTime'),
                                                                              getattr(ship_from_location_businessHours,
                                                                                      'timeZoneOffset'),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults"
                                                                                  ".timeZone"),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults."
                                                                                  "tmDateFormat"),
                                                                              look_up_dict.get(
                                                                                  "configuration.systemDefaults"
                                                                                  ".gS1DateFormat")
                                                                              )

            pickup_to_dates_times = operation_utils.OperationUtils.convertGs1ToLocationDate(getattr(ship_dates_info, 'endDate'),
                                                                            getattr(ship_dates_info, 'endTime'),
                                                                            getattr(ship_from_location_businessHours,
                                                                                    'timeZoneOffset'),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults"
                                                                                ".timeZone"),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults."
                                                                                "tmDateFormat"),
                                                                            look_up_dict.get(
                                                                                "configuration.systemDefaults"
                                                                                ".gS1DateFormat")
                                                                            )

        else:
            pickup_from_dates_times = getattr(ship_dates_info, 'beginDate') + 'T' + getattr(ship_dates_info,
                                                                                            'beginTime')
            pickup_to_dates_times = getattr(ship_dates_info, 'endDate') + 'T' + getattr(ship_dates_info,
                                                                                        'endTime')

        expected_result_dict.update({'pickupFromDateTime': pickup_from_dates_times,
                                     'pickupToDateTime': pickup_to_dates_times})
    logger.debug(f"Calculated dates based on the settings is : {expected_result_dict}", message_hdr_info)
    return expected_result_dict


def attach_shipment_reference_number(*args):
    """
     This function is used for creating the shipment level reference number
     for order Release.
     :param args: list of a tuple of item(shipFromLocation, tmCustomerCode,
     tmCustomerCode from the customer enrichment info, orderId,supplierId,tmReferenceNumber,
     orderTypeCode,message_hdr_info)
     :return: dictionary of the calculated referenceNumber structure.
    """
    shipFromLocation = args[0][0]
    tmCustomerCode = args[0][1] if args[0][1] else args[0][2]
    orderId = args[0][3]
    supplierId = args[0][4]
    tmReferenceNumber = args[0][5] if args[0][5] else []
    orderTypeCode = args[0][6]
    message_hdr_info = args[0][7]
    reference_number_structure = []
    if shipFromLocation:
        reference_number_structure.append({
            "referenceNumberTypeCode": "WM_WH_ID_",
            "referenceNumber": shipFromLocation
        })
    if tmCustomerCode:
        reference_number_structure.append({
            "referenceNumberTypeCode": "WM_CLT_ID_",
            "referenceNumber": tmCustomerCode
        })
    if orderId:
        reference_number_structure.append({
            "referenceNumberTypeCode": "WM_ORDNUM_",
            "referenceNumber": orderId
        })
    if look_up_dict.get("OrderTypeCode", {}).get(orderTypeCode):
        reference_number_structure.append({
            "referenceNumberTypeCode": "DCS_ORD_CAT_",
            "referenceNumber": look_up_dict.get("OrderTypeCode", {}).get(orderTypeCode)
        })
    if supplierId:
        reference_number_structure.append({
            "referenceNumberTypeCode": "DCS_ORD_SUP",
            "referenceNumber": supplierId
        })
    for tm_ref_number in tmReferenceNumber:
        reference_number_structure.append({
            "referenceNumberTypeCode": getattr(tm_ref_number, "referenceNumberName"),
            "referenceNumber": getattr(tm_ref_number, "referenceNumberValue")
        })
    logger.debug(f'Calculated referenceNumber structure for shipment: {orderId} is: {reference_number_structure}',
                 message_hdr_info)
    return {"referenceNumberStructure": reference_number_structure}


def get_order_note_memo(*args):
    """
        This function is used for getting the memo details.
        :param args: list of a tuple of item(orderNote,message_hdr_info)
        :return: dictionary of the order type memo.
    """
    orderNote = args[0][0] if args[0][0] else []
    message_hdr_info = args[0][1]
    memo = list(filter(lambda note: getattr(note, "type", None) == "MEMO", orderNote))
    order_note_dict = {}
    if len(memo) > 0:
        printable_memo = list(filter(lambda note: getattr(note, "isPrintable", None) == True, memo))
        non_printable_memo = list(
            filter(lambda note: getattr(note, "isPrintable", None) == False, memo))
        if printable_memo:
            text = getattr(printable_memo[0], "text")
            if text and getattr(text, "value"):
                order_note_dict.update({"printableMemo": getattr(text, "value")})
        if non_printable_memo:
            text = getattr(non_printable_memo[0], "text")
            if text and getattr(text, "value"):
                order_note_dict.update({"nonPrintableMemo": getattr(text, "value")})
        logger.debug(f'Calculated order note is: {order_note_dict}', message_hdr_info)
        return order_note_dict
    return None


def get_alternate_value(*args):
    """
    This function is used for choosing the alternate item if first one is none get the second
    :param: list of a tuple of item(variable 1, variable 2,message_hdr_info)
    """
    return args[0][0] if args[0][0] else args[0][1]


def get_freight_class_code(*args):
    """
        This function is used for calculating the freightClassCode for a given input
        for order Release.
        :param args: list of a tuple of item(freightClassCode, itemMaster enrichment)
        :return: converted freightClassCode.
    """
    freight_class_code = args[0][0]
    item_number = args[0][1]
    item_master_enrichment = args[0][2]
    message_hdr_info = args[0][3]
    if freight_class_code:
        return freight_class_code
    elif not freight_class_code and item_master_enrichment:
        for obj in item_master_enrichment:
            if eval('obj.itemNumber') == item_number:
                enriched_freight_class_code = eval('obj.freightClassCode')
                logger.debug(
                    f"Fetched freightClassCode for item {item_number} is: "
                    f"{enriched_freight_class_code} ", message_hdr_info)
                return enriched_freight_class_code
    else:
        return "*FAK"


def get_calculated_weight(*args):
    """
        This function is used for calculating the weight and volume for a given input
        for order Release.
        :param args: list of a tuple of item(uom_category,source_uom,target_uom,value,measurement_type,line_quantity,
        message_hdr_info)
        :return: converted weight/volume.
    """
    uom_category = args[0][0]
    source_uom = args[0][1]
    target_uom = args[0][2]
    value = args[0][3]
    measurement_type = args[0][4]
    line_quantity = args[0][5]
    item_master_enrichment = args[0][6]
    item_number = args[0][7]
    message_hdr_info = args[0][8]

    if value:
        calculated_value = operation_utils.OperationUtils.getUomConversion(uom_category, source_uom, target_uom, value)
        if measurement_type == "TOTAL_GROSS_WEIGHT" and line_quantity > 0:
            calculated_value = calculated_value / line_quantity
        if uom_category and uom_category.lower() == "volume" and calculated_value and line_quantity > 0:
            calculated_value = calculated_value / line_quantity
        logger.debug(f"Converted {uom_category} for {value} from source uom: {source_uom} to target_uom: {target_uom} is: "
                     f"{calculated_value} ", message_hdr_info)
        return calculated_value
    elif not value and item_master_enrichment and uom_category.lower() == "weight":
        for obj in item_master_enrichment:
            if eval('obj.itemNumber') == item_number:
                calculated_value = eval('obj.nominalWeight')*line_quantity
                logger.debug(
                    f"Fetched nominal weight calculation for item {item_number} is: "
                    f"{calculated_value} ", message_hdr_info)
                return calculated_value


def get_calculated_volume(*args):
    """
        This function is used for calculating the volume for a given input
        for order Release.
        :param args: list of a tuple of item(uom_category,source_uom,target_uom,value,line_quantity,
        message_hdr_info)
        :return: converted volume.
    """
    uom_category = args[0][0]
    source_uom = args[0][1]
    target_uom = args[0][2]
    value = args[0][3]
    line_quantity = args[0][4]
    unit_mucode = args[0][5]
    unit_value = args[0][6]
    message_hdr_info = args[0][7]

    if unit_value not in [None, '']:
        value = unit_value
    if unit_mucode not in [None, '']:
        source_uom = unit_mucode

    calculated_value = operation_utils.OperationUtils.getUomConversion(uom_category, source_uom, target_uom, value)

    line_quantity = 1 if unit_value else line_quantity

    if uom_category and uom_category.lower() == "volume" and calculated_value and line_quantity > 0:
        calculated_value = calculated_value / line_quantity
    logger.debug(f"Converted {uom_category} for {value} from source uom: {source_uom} to target_uom: {target_uom} is: "
                 f"{calculated_value} ", message_hdr_info)
    return calculated_value


def get_container_item_flag(*args):
    """
        This function is used for calculating the container item flag
        for order Release.
        :param args: list of a tuple of item(item_level_detail_enum_val,shipment_entry_mode,message_hdr_info)
        :return: True if item_level_detail_enum_val == "ILD_OPTIONAL" and
         shipment_entry_mode == "TOM_ITEM_LEVEL_SUMMARY" is False else None.
    """
    item_level_detail_enum_val = args[0][0]
    shipment_entry_mode = args[0][1]
    if item_level_detail_enum_val == "ILD_OPTIONAL" and shipment_entry_mode == "TOM_ITEM_LEVEL_SUMMARY":
        return None
    return True


def get_container_id_for_line(*args):
    container_list = args[0][0]
    itemNumber = args[0][1]
    if container_list:
        for container in container_list:
            for reference_numbers in getattr(container, "referenceNumberStructure", None):
                if getattr(reference_numbers, "referenceNumberTypeCode", None) == "WM_ORDLIN_" \
                        and getattr(reference_numbers, "referenceNumber", None) == itemNumber:
                    return getattr(container, "systemContainerID", None)
    return None


def attach_container_reference_number(*args):
    """
    This function is used for calculating the line level reference number
    for order Release.
    :param args: list of a tuple of item(lineNumber,itemNumber,subLineId,isOrderLineAllocatable,tmReferenceNumber,
    message_hdr_info)
    :return: dictionary of a referenceNumber structure.
    """
    lineNumber = args[0][0]
    itemNumber = args[0][1]
    subLineId = args[0][2]
    isOrderLineAllocatable = args[0][3]
    tmReferenceNumber = args[0][4] if args[0][4] else []
    message_hdr_info = args[0][5]
    ref_numbers = []
    if lineNumber:
        ref_numbers.append({
            "referenceNumberTypeCode": "WM_ORDLIN_",
            "referenceNumber": lineNumber
        })
    if itemNumber:
        ref_numbers.append({
            "referenceNumberTypeCode": "DCS_ITEM_ID_",
            "referenceNumber": itemNumber
        })
    if isOrderLineAllocatable is False or isOrderLineAllocatable in ['false', False]:
        ref_numbers.append({
            "referenceNumberTypeCode": "DCS_NON_ALLO",
            "referenceNumber": True
        })
    ref_numbers.append({
        "referenceNumberTypeCode": "WM_ORDSLN_",
        "referenceNumber": subLineId if subLineId else 0
    })

    for tmReferenceNumber in tmReferenceNumber:
        if getattr(tmReferenceNumber, "referenceNumberName") and getattr(tmReferenceNumber, "referenceNumberValue"):
            ref_numbers.append({
                "referenceNumberTypeCode": getattr(tmReferenceNumber, "referenceNumberName"),
                "referenceNumber": getattr(tmReferenceNumber, "referenceNumberValue")
            })
    logger.debug(f"Calculated reference number for line: {lineNumber} is: {ref_numbers}", message_hdr_info)
    return {"referenceNumberStructure": ref_numbers} if len(ref_numbers) else None


def get_shipment_details_for_wms_host(enrichment_data):
    """
    This function is used for getting the correct enriched shipment details for a given orderId for order Release.
    :param args: list of a tuple of item(orderId, shipments enrichment data, message hdr info)
    :return: enriched shipment details from the shipments enrichment object
    """
    converted_data = operation_utils.OperationUtils.namespace_to_dict(enrichment_data)
    if converted_data:
        for data in converted_data:
            if 'shipmentLeg' in data:
                del data['shipmentLeg']
    return converted_data


def get_container_id_for_order_line_cancel(*args):
    """
    This function is used to form correct system container id for order line delete
    :param args: list of a tuple of item(orderId, shipments enrichment data, message hdr info)
    :return: enriched shipment details from the shipments enrichment object
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]
    try:
        entity_data = shipment_enrichment
        line_item = eval('orderReleaseMessage[0].lineItem')
        container = eval('entity_data[0].container')

        line_item_numbers = set(
            int(eval("x.lineItemNumber")) for x in list(filter(lambda x: eval("x.actionCode") == "CANCEL", line_item)))

        for element in container:
            for reference in eval('element.referenceNumberStructure'):
                if eval('reference.referenceNumberTypeCode') == "WM_ORDLIN_":
                    if int(eval('reference.referenceNumber')) in line_item_numbers:
                        return eval('element.systemContainerID')

        return None
    except Exception as e:
        logger.error(f'Exception errors order cancel flow {e}', message_hdr_info)


def get_container_id_for_order_line_delete(*args):
    """
    This function is used to form correct system container id for order line delete
    :param args: list of a tuple of item(orderId, shipments enrichment data, message hdr info)
    :return: enriched shipment details from the shipments enrichment object
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []

    entity_data = shipment_enrichment
    line_item = eval('orderReleaseMessage[0].lineItem')
    container = eval('entity_data[0].container')

    line_item_numbers = set(
        int(eval("x.lineItemNumber")) for x in list(filter(lambda x: eval("x.actionCode") == "DELETE", line_item)))

    for element in container:
        for reference in eval('element.referenceNumberStructure'):
            if eval('reference.referenceNumberTypeCode') == "WM_ORDLIN_":
                if int(eval('reference.referenceNumber')) in line_item_numbers:
                    return eval('element.systemContainerID')

    return None


def get_item_level_id_code_from_wms(*args):
    """
        This function is contentLevelTypeCode from enrichment shipment details.
    """

    shipment_details = args[0][0]
    container = eval('shipment_details[0].container')
    try:
        for element in container:
            return eval('element.contentLevelTypeCode')
    except Exception as ex:
        return None


def get_frieght_class_code_from_wms(*args):
    """
        This function is freightClassCode from enrichment shipment details.
    """

    shipment_details = args[0][0]
    container = eval('shipment_details[0].container')
    try:
        for element in container:
            return (eval('element.weightByFreightClass[0].freightClassCode'))
    except Exception as ex:
        return None


def get_item_description_from_wms(*args):
    """
        This function is itemDescription from enrichment shipment details.
    """

    shipment_details = args[0][0]
    container = eval('shipment_details[0].container')
    try:
        for element in container:
            return eval('element.itemDescription')
    except Exception as ex:
        return None


"""
Returns first element in list
"""


def get_first_element(*args):
    additionalLocationIdList = args[0][0] if args[0][0] else []
    if additionalLocationIdList not in ["", None] and isinstance([], list):
        return additionalLocationIdList[0]
    elif additionalLocationIdList:
        return additionalLocationIdList
    return None


def check_flag(*args):
    shipmentThroughPoint = args[0][0]
    if shipmentThroughPoint not in [None, '', []]:
        for stp in eval('shipmentThroughPoint'):
            additional_location_id = getattr(stp, "additionalLocationId")

            if len(additional_location_id) > 0:
                return False
        return True
    return True



"""##############################OrderRelease functions END ##########################################"""
