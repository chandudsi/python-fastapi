from neo.log import Logger


def check_won_status_Code(*args):
    """
    This function checks for valid Status Codes and then returns True if the status is in STOCK_ALLOCATED, PRODUCT_PICKED
    :param args: A variable-length argument list. It is assumed that the first argument (*args[0])
               contains the payload and the last argument contains message_hdr_info.
    :return:
        bool: True if status code is in STOCK_ALLOCATED and PRODUCT_PICKED
    """

    payload = args[0][0]

    message_hdr_info = args[0][-1]

    qualifier_name = "check_won_status_Code"
    Logger.info(f"{qualifier_name}, Running, {message_hdr_info}")

    won_status_codes_list = ['PRODUCT_PICKED', 'STOCK_ALLOCATED', 'PICKING_UNSUCCESSFUL',
                             'STOCK_ALLOCATION_UNSUCCESSFUL', 'PALLET_ESTIMATED']
    try:
        won_status_code = eval('payload.event.warehousingOutboundNotification[0].shipment[0].warehousingOutboundStatusCode')
    except Exception as e:
        Logger.error(f"{qualifier_name} Not able to find the Status Code of WON due to: {e}", message_hdr_info)
        Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
        return False

    if won_status_code in won_status_codes_list:
        if won_status_code in ['STOCK_ALLOCATION_UNSUCCESSFUL', 'PICKING_UNSUCCESSFUL']:
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return False
        else:
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return True
    else:
        Logger.error(f"{qualifier_name} Incorrect Warehousing status code received! please send the status code in "
                     f"the below mentioned list\n status_codes_list = {won_status_codes_list}", message_hdr_info)
        Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
        return False


def check_won_status_Code_wave_pick_cancel(*args):
    """
    This function checks for valid Status Codes and then returns False if the status is in STOCK_ALLOCATED, PRODUCT_PICKED
    :param args: A variable-length argument list. It is assumed that the first argument (*args[0])
               contains the payload and the last argument contains message_hdr_info.
    :return:
        bool: False if status code is in STOCK_ALLOCATED and PRODUCT_PICKED
    """
    payload = args[0][0]

    message_hdr_info = args[0][-1]

    qualifier_name = "check_won_status_Code_wave_pick_cancel"
    Logger.info(f"{qualifier_name}, Running, {message_hdr_info}")

    won_status_codes_list = ['PRODUCT_PICKED', 'STOCK_ALLOCATED', 'PICKING_UNSUCCESSFUL',
                             'STOCK_ALLOCATION_UNSUCCESSFUL', 'PALLET_ESTIMATED']
    try:
        won_status_code = eval('payload.event.warehousingOutboundNotification[0].shipment[0].warehousingOutboundStatusCode')
    except Exception as e:
        Logger.error(f"{qualifier_name} Not able to find the Status Code of WON due to: {e}", message_hdr_info)
        Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
        return False

    if won_status_code in won_status_codes_list:
        if won_status_code in ['STOCK_ALLOCATION_UNSUCCESSFUL', 'PICKING_UNSUCCESSFUL']:
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return True
        else:
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return False
    else:
        Logger.error(f"Incorrect Warehousing status code received! please send the status code in "
                     f"the below mentioned list\n status_codes_list = {won_status_codes_list}")
        Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
        return False


def check_won_status_code_product_picked(*args):
    """
    This function checks for valid Status Codes and then returns True if the status is PRODUCT_PICKED
    :param args: A variable-length argument list. It is assumed that the first argument (*args[0])
               contains the payload and the last argument contains message_hdr_info.
    :return:
        bool: True if status code is PRODUCT_PICKED
    """
    payload = args[0][0]

    message_hdr_info = args[0][-1]

    qualifier_name = "check_won_status_code_product_picked"
    Logger.info(f"{qualifier_name}, Running, {message_hdr_info}")

    try:
        shipment_item = eval(
                    'payload.event.warehousingOutboundNotification[0].shipment[0].shipmentItem')
    except Exception as ex:
        Logger.error(f"{qualifier_name} Not able to find the shipmentItem information due to: {ex}", message_hdr_info)
        Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
        shipment_item = None

    if shipment_item not in [None, '']:

        won_status_codes_list = ['PRODUCT_PICKED', 'STOCK_ALLOCATED', 'PICKING_UNSUCCESSFUL',
                                 'STOCK_ALLOCATION_UNSUCCESSFUL', 'PALLET_ESTIMATED']
        try:
            won_status_code = eval(
                'payload.event.warehousingOutboundNotification[0].shipment[0].warehousingOutboundStatusCode')
        except Exception as e:
            Logger.error(f"{qualifier_name} Not able to find the Status Code of WON due to: {e}", message_hdr_info)
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return False

        if won_status_code in won_status_codes_list:
            if won_status_code in ['STOCK_ALLOCATION_UNSUCCESSFUL', 'PICKING_UNSUCCESSFUL',
                                   'STOCK_ALLOCATED', 'PALLET_ESTIMATED']:
                Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
                return False
            else:

                Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
                return True
        else:
            Logger.error(f"{qualifier_name} Incorrect Warehousing status code received! please send the status code in "
                         f"the below mentioned list\n status_codes_list = {won_status_codes_list}", message_hdr_info)
            Logger.info(f"{qualifier_name}, END, {message_hdr_info}")
            return False
    return False
