from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger

look_up_dict = adapter_cps_provider.get_properties().get("codMapConfig")

"""##############################OrderRelease functions start ##########################################"""


def check_document_action_code_create(*args):
    """
    This function is used checking the source Condition for creating a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]
    for order in orderReleaseMessage:
        orderId = eval('order.orderId')
        is_shipment_exist = list(
            filter(lambda shipment: eval('shipment.shipmentNumber') == orderId, shipment_enrichment))
        if eval('order.documentActionCode') == "ADD" and len(is_shipment_exist) == 0:
            logger.info(f"Shipment create flow is enabled", message_hdr_info)
            return True
    return False


def is_order_line_cancel_exist(*args):
    """
    This function is used for checking is there any line which is having the action code CANCEL for order Release
    :param args: list of a tuple of item(order Release)
    :return: if there is line in cancel state then first return will be True else False
             if total cancelled line is equal to total line coming in the message the True else False
    """
    order_details = args[0]
    is_order_line_cancel = False
    total_cancelled_line = 0
    for line in getattr(order_details, "lineItem", []):
        if getattr(line, "actionCode", None) == "CANCEL":
            total_cancelled_line += 1
            is_order_line_cancel = True

    return is_order_line_cancel, total_cancelled_line == len(getattr(order_details, "lineItem", []))


def is_order_line_delete_exist(*args):
    """
    This function is used for checking is there any line which is having the orderline delete for order Release
    :param args: list of a tuple of item(order Release)
    :return: if there is less lien item in orderRelease update message then actual
    """
    order_details = args[0]
    shipment_details = args[1]
    try:
        container = eval('shipment_details[0].container')

        total_input_message_line = 0
        line_item_numbers = set()
        for line in getattr(order_details, "lineItem", []):
                total_input_message_line += 1
                line_item_numbers.add(eval('line.lineItemNumber'))

        total_enrichment_call_line_actual = 0
        total_enrichment_call_line_extra = 0
        for element in container:
            for reference in eval('element.referenceNumberStructure'):
                if eval('reference.referenceNumberTypeCode') == "WM_ORDLIN_":
                    if int(eval('reference.referenceNumber')) in line_item_numbers:
                        total_enrichment_call_line_actual += 1
                    else:
                        total_enrichment_call_line_extra += 1

        final_total_enrichment_line = total_enrichment_call_line_actual + \
                                      total_enrichment_call_line_extra


        return True if total_input_message_line < final_total_enrichment_line else False
    except Exception as ex:
        return False



def execute_the_precondition_for_consolidation_on_flow(*args):
    """
    This function is used for checking the precondition for allowing the shipment update/cancel for order Release
    It's like when shipment is allocated in the WM and shipment reference is having the code WM_SHP_STS and
    it's value contains the property value configured in the codmap for stock allocated
    :param args: list of a tuple of item(shipment_details)
    :return: None if pre-conditions are satisfied else raise the  exception
    """
    shipment_details = args[0]
    shipFromLocation = args[1]
    stockAllocatedRef = look_up_dict.get("tms.defaults.shipmentStatus.stockAllocated")
    try:
        stop_details = eval('shipment_details[0].shipmentLeg[0].load.stop')
    except Exception as ex:
        stop_details = []
    for stop in stop_details:
        if getattr(stop, "shippingLocationCode", None) == shipFromLocation:
            ref_numbers = getattr(stop, "referenceNumberStructure", [])
            wm_stop_status = list(filter(
                lambda ref_number: getattr(ref_number, "referenceNumberTypeCode", None) == "WM_SHP_STS",
                ref_numbers))
            if len(wm_stop_status) and getattr(wm_stop_status[0], "referenceNumber") in stockAllocatedRef:
                raise ValueError("Failed to cancel or delete TM shipment Line, as one or more lines for "
                                 "the associated stop are already Allocated shipment in WM.")


def execute_the_precondition_for_shipment_update_flow(*args):
    """
    This function is used for checking the precondition for allowing the shipment update/cancel for order Release
    It's like when shipment is allocated in the WM and shipment reference is having the code WM_SHIPMENT and
    it's value contains the property value configured in the codmap for stock allocated, productPickedRef,
    productPickedCancelled
    :param args: list of a tuple of item(shipment_details)
    :return: None if pre-conditions are satisfied else raise the  exception
    """
    shipment_details = args[0]
    ref_numbers = getattr(shipment_details[0], "referenceNumberStructure", [])
    stockAllocatedRef = look_up_dict.get("tms.defaults.shipmentStatus.stockAllocated")
    productPickedRef = look_up_dict.get("tms.defaults.shipmentStatus.productPicked")
    productPickedCancelled = look_up_dict.get("tms.defaults.shipmentStatus.pickCancelled")
    for ref_number in ref_numbers:
        if getattr(ref_number, "referenceNumberTypeCode", None) == 'WM_SHIPMENT':
            if getattr(ref_number, "referenceNumber", None) in [stockAllocatedRef, productPickedRef,
                                                                productPickedCancelled]:
                raise ValueError("Failed to cancel or delete TM shipment Line, as one or more lines for "
                                 "the associated stop are already Allocated shipment in WM.")


def check_document_action_code_cancel(*args):
    """
    This function is used checking the source Condition for updating a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]

    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system is None:
        for order in orderReleaseMessage:
            orderId = eval('order.orderId')
            _, is_order_cancel = is_order_line_cancel_exist(order)
            shipment_details = list(
                filter(lambda shipment: eval('shipment.shipmentNumber') == orderId, shipment_enrichment))
            if ((eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and is_order_cancel) or
                eval('order.documentActionCode') in ["CANCEL"]) and len(shipment_details) > 0:
                shipFromLocation = getattr(shipment_details[0], 'shipFromLocationCode', None)
                isConsolidationEnabled = look_up_dict.get("warehouses", {}). \
                    get(shipFromLocation)
                shipmentLeg = getattr(shipment_details[0], "shipmentLeg")
                load_details = getattr(shipmentLeg[0], 'load', None)
                serviceCode = None
                if load_details:
                    serviceCode = getattr(load_details, 'serviceCode')
                if not serviceCode:
                    serviceCode = getattr(shipmentLeg[0], 'serviceCode', None)

                tmLoadServiceType = look_up_dict.get("TransportServiceTransitModeFromAppToGS1", {}). \
                    get(serviceCode, look_up_dict.get("tms.defaults.loadType"))
                if isConsolidationEnabled or tmLoadServiceType in ['3', '4']:
                    execute_the_precondition_for_consolidation_on_flow(shipment_details, shipFromLocation)
                else:
                    execute_the_precondition_for_shipment_update_flow(shipment_details)
                logger.info(f"Shipment Cancel flow is enabled", message_hdr_info)
                return True
            # if a non existing shipment attempted for cancel fail it
            elif ((eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and is_order_cancel) or
                  eval('order.documentActionCode') in ["CANCEL"]) and len(shipment_details) == 0:
                logger.error(f"Shipment can not be cancelled or deleted as it does not exist or already cancelled or "
                             "deleted", message_hdr_info)
                raise ValueError("Shipment can not be cancelled or deleted as it does not exist or already cancelled or "
                                 "deleted")
        return False

    return False


def check_document_action_code_update(*args):
    """
    This function is used checking the source Condition for cancelling a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]

    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system is None:
        for order in orderReleaseMessage:
            orderId = eval('order.orderId')
            order_line_cancel_exist, is_order_cancel = is_order_line_cancel_exist(order)
            if is_order_cancel:
                return False
            shipment_details = list(
                filter(lambda shipment: eval('shipment.shipmentNumber') == orderId, shipment_enrichment))
            delete_order_line_exist = is_order_line_delete_exist(order, shipment_details)

            if eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and len(shipment_details) > 0:
                shipFromLocation = getattr(shipment_details[0], 'shipFromLocationCode', None)
                isConsolidationEnabled = look_up_dict.get("warehouses", {}). \
                    get(shipFromLocation)
                shipmentLeg = getattr(shipment_details[0], "shipmentLeg")
                load_details = getattr(shipmentLeg[0], 'load', None)
                serviceCode = None
                if load_details:
                    serviceCode = getattr(load_details, 'serviceCode')
                if not serviceCode:
                    serviceCode = getattr(shipmentLeg[0], 'serviceCode', None)
                tmLoadServiceType = look_up_dict.get("TransportServiceTransitModeFromAppToGS1", {}). \
                    get(serviceCode, look_up_dict.get("tms.defaults.loadType"))
                if (isConsolidationEnabled or tmLoadServiceType in ['3', '4']) and order_line_cancel_exist:
                    execute_the_precondition_for_consolidation_on_flow(shipment_details, shipFromLocation)
                elif order_line_cancel_exist or delete_order_line_exist:
                    execute_the_precondition_for_shipment_update_flow(shipment_details)
                logger.info(f"Shipment Update flow is enabled", message_hdr_info)
                return True
            # if a non existing shipment attempted for update fail it
            elif eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and len(shipment_details) == 0:
                logger.error(f"Shipment can not be cancelled or deleted as it does not exist or "
                             f"already cancelled or deleted"
                             , message_hdr_info)
                raise ValueError("Shipment can not be cancelled or deleted as it does not exist or already cancelled or "
                                 "deleted")
        return False

    return False

def check_document_action_code_delete(*args):
    """
    This function is used checking the source Condition for deleting a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]

    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system is None:

        function_name = 'check_document_action_code_delete'
        for order in orderReleaseMessage:
            orderId = eval('order.orderId')
            _, is_order_cancel = is_order_line_cancel_exist(order)
            shipment_details = list(
                filter(lambda shipment: eval('shipment.shipmentNumber') == orderId, shipment_enrichment))
            if eval('order.documentActionCode') in ["DELETE"] and len(shipment_details) > 0:
                shipFromLocation = getattr(shipment_details[0], 'shipFromLocationCode', None)
                isConsolidationEnabled = look_up_dict.get("warehouses", {}). \
                    get(shipFromLocation)
                shipmentLeg = getattr(shipment_details[0], "shipmentLeg")
                load_details = getattr(shipmentLeg[0], 'load', None)
                serviceCode = None
                if load_details:
                    serviceCode = getattr(load_details, 'serviceCode')
                if not serviceCode:
                    serviceCode = getattr(shipmentLeg[0], 'serviceCode', None)

                tmLoadServiceType = look_up_dict.get("TransportServiceTransitModeFromAppToGS1", {}). \
                    get(serviceCode, look_up_dict.get("tms.defaults.loadType"))
                if isConsolidationEnabled or tmLoadServiceType in ['3', '4']:
                    execute_the_precondition_for_consolidation_on_flow(shipment_details, shipFromLocation)
                else:
                    execute_the_precondition_for_shipment_update_flow(shipment_details)
                logger.info(f"Shipment Delete flow is enabled", message_hdr_info)
                return True
            # if a non existing shipment attempted for delete fail it
            elif eval('order.documentActionCode') == "DELETE" and len(shipment_details) == 0:
                logger.error(f"Shipment can not be cancelled or deleted as it does not exist or "
                             f"already cancelled or deleted"
                             , message_hdr_info)
                raise ValueError("Shipment can not be cancelled or deleted as it does not exist or already cancelled or "
                                 "deleted")
        return False

    return False



def check_document_action_code_order_cancel_from_wms(*args):
    """
        This function is used checking the source Condition for updating a shipment for order Release
        :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
        :return: True if shipment call needs to be called else False
        """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    message_hdr_info = args[0][2]

    # checking sender system is from WM or not
    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system == "WM":
        try:
            # two cases here orderCancel and orderLineCancel for sender system WM
            for order in orderReleaseMessage:
                lineItem = eval('order.lineItem')
                lineCancelItem = list(
                    filter(lambda x: eval("x.actionCode") == "CANCEL", eval('order.lineItem')))

                # this case is for orderCancel
                if (eval('order.documentActionCode') == "CANCEL"):
                    logger.info("OrderRelease Order Cancel flow for host WMS is enabled", message_hdr_info)
                    return True

            return False
        except Exception:
            return False

    return False


def check_document_action_code_orderLine_cancel_from_wms(*args):
    """
    This function is used checking the source Condition for updating a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    message_hdr_info = args[0][2]

    # checking sender system is from WM or not
    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system == "WM":
        try:
            # two cases here orderCancel and orderLineCancel for sender system WM
            for order in orderReleaseMessage:
                _, is_order_cancel = is_order_line_cancel_exist(order)
                lineItem = eval('order.lineItem')
                lineCancelItem = list(
                    filter(lambda x: eval("x.actionCode") == "CANCEL", eval('order.lineItem')))

                order_cancel_check = False

                # this case is for orderCancel
                if eval('order.documentActionCode') == "CANCEL":
                    order_cancel_check = True

                # this is Case for orderlineCancel
                if ((eval('order.documentActionCode') == "PARTIAL_CHANGE" or eval(
                        'order.documentActionCode') == "CHANGE_BY_REFRESH") and
                    (order_cancel_check is False) and (len(lineCancelItem) > 0)):
                    logger.info("OrderRelease OrderLine Cancel flow for host WMS is enabled", message_hdr_info)
                    return True

            return False
        except Exception:
            return False

    return False


def check_document_action_code_order_delete_from_wms(*args):
    """
    This function is used checking the source Condition for deleting a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    message_hdr_info = args[0][2]

    # checking sender system is from WM or not
    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system == "WM":
        try:
            for order in orderReleaseMessage:
                # case for order delete from WMS
                if eval('order.documentActionCode') == "DELETE":
                    logger.info("OrderRelease Order Delete flow for host WMS is enabled", message_hdr_info)
                    return True

            return False
        except Exception:
            return False
    return False


def check_document_action_code_orderLine_delete_from_wms(*args):
    """
    This function is used checking the source Condition for deleting a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1]
    message_hdr_info = args[0][2]

    # checking sender system is from WM or not
    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system == "WM":
        try:
            for order in orderReleaseMessage:
                _, is_order_cancel = is_order_line_cancel_exist(order)

                lineDeleteItem = list(
                    filter(lambda x: eval("x.actionCode") == "DELETE", eval('order.lineItem')))

                # case for order line delete from WMS
                if eval('order.documentActionCode') == "PARTIAL_CHANGE" and \
                        (len(lineDeleteItem) > 0) and (is_order_cancel is False):
                    logger.info("OrderRelease OrderLine Delete flow for host WMS is enabled", message_hdr_info)
                    return True

                elif (len(eval('shipment_enrichment[0].container')) > len(eval('order.lineItem'))) \
                        and (is_order_cancel is False):
                    logger.info("OrderRelease OrderLine Delete flow for host WMS is enabled", message_hdr_info)
                    return True

            return False
        except Exception:
            return False
    return False



def check_document_action_code_update_from_wms(*args):
    """
    This function is used checking the source Condition for cancelling a shipment for order Release
    :param args: list of a tuple of item(order Release, shipment enrichment data, message hdr info)
    :return: True if shipment call needs to be called else False
    """
    orderReleaseMessage = args[0][0] if args[0][0] else []
    shipment_enrichment = args[0][1] if args[0][1] else []
    message_hdr_info = args[0][2]

    sender_system = None
    try:
        sender_system = eval('orderReleaseMessage[0].senderSystem')
    except:
        pass

    if sender_system == "WM":
        for order in orderReleaseMessage:
            orderId = eval('order.orderId')
            order_line_cancel_exist, is_order_cancel = is_order_line_cancel_exist(order)
            if is_order_cancel:
                return False
            shipment_details = list(
                filter(lambda shipment: eval('shipment.shipmentNumber') == orderId, shipment_enrichment))
            if eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and len(shipment_details) > 0:
                shipFromLocation = getattr(shipment_details[0], 'shipFromLocationCode', None)
                isConsolidationEnabled = look_up_dict.get("warehouses", {}). \
                    get(shipFromLocation)
                shipmentLeg = getattr(shipment_details[0], "shipmentLeg")
                load_details = getattr(shipmentLeg[0], 'load', None)
                serviceCode = None
                if load_details:
                    serviceCode = getattr(load_details, 'serviceCode')
                if not serviceCode:
                    serviceCode = getattr(shipmentLeg[0], 'serviceCode', None)
                tmLoadServiceType = look_up_dict.get("TransportServiceTransitModeFromAppToGS1", {}). \
                    get(serviceCode, look_up_dict.get("tms.defaults.loadType"))
                if (isConsolidationEnabled or tmLoadServiceType in ['3', '4']) and order_line_cancel_exist:
                    execute_the_precondition_for_consolidation_on_flow(shipment_details, shipFromLocation)
                logger.info(f"Shipment Update flow is enabled from WMS", message_hdr_info)
                return True
            # if a non existing shipment attempted for update fail it
            elif eval('order.documentActionCode') == "CHANGE_BY_REFRESH" and len(shipment_details) == 0:
                logger.error(f"Shipment can not be cancelled or deleted as it does not exist or "
                             f"already cancelled or deleted from WMS"
                             , message_hdr_info)
                raise ValueError("Shipment can not be cancelled or deleted as it does not exist or already cancelled or "
                                 "deleted")
        return False

    return False





"""##############################OrderRelease functions END ##########################################"""
