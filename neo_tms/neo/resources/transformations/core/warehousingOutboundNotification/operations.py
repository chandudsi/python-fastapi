from neo.log import Logger
import types
import neo.process.transformation.utils.operation_util as operation_utils
from neo.connect.cps import adapter_cps_provider
import json

"""############################## WON functions START ##########################################"""


def tms_validate_won_ids(*args):
    """
    Validate the Work Order Number (WON) IDs in the Transportation Management System (TMS).

    This function checks if mandatory elements, such as load_id and shipping_location, are present
    and not None or empty. It logs messages to provide information about the validation process.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0])
               contains the shipping_location, load_id, and message_hdr_info in that order.

    :return:
        bool: True if both load_id and shipping_location are provided and not None or empty, indicating
              successful validation. False otherwise.
    """
    shipping_location = args[0][0]
    load_id = args[0][1]
    message_hdr_info = args[0][-1]

    qualifier_name = 'tms_validate_won_ids'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    if not (load_id not in [None, ''] and shipping_location not in [None, '']):
        Logger.error(f"{qualifier_name} Not able to proceed further, as the mandatory elements are missing!!")
        return False
    else:
        Logger.debug("Validated the routeId, shipFromLocationCode successfully", message_hdr_info)
        Logger.debug(f'{qualifier_name}, End, {message_hdr_info}')
        return True


def get_reference_number_structure(shipment_document):
    """
    Get the reference number structure from a shipment document if it exists.

    :param shipment_document: (dict) The shipment document containing reference number information.

    :returns:
        dict or None: A reference number structure dictionary if found, otherwise None.
    """
    try:
        # Attempt to extract the reference number structure from the shipment document.
        ref_num_struct = eval('shipment_document.referenceNumberStructure')
    except Exception as e:
        ref_num_struct = None

    if ref_num_struct:
        # Iterate through the reference number structures and find the one with type 'WM_SHIPMENT'.
        return next((eval('ref_num') for ref_num in ref_num_struct if
                     eval('ref_num.referenceNumberTypeCode') == 'WM_SHIPMENT'), None)
    else:
        # If no reference number structure was found, return None.
        return None


def reference_number_update_data_shipment(reference_number_structure, shipment_document, status_code,
                                          stockAllocatedShipmentReferenceNumber, productPickedShipmentReferenceNumber,
                                          referenceNumberActionEnumVal, waveOrPickCancelledShipmentReferenceNumber,
                                          message_hdr_info):
    """
    Generate reference number update data for a shipment based on the provided parameters.

    :param reference_number_structure: (dict) The reference number structure associated with the shipment.
    :param shipment_document: (dict) The shipment document for which reference number data is being generated.
    :param status_code: (str) The status code indicating the current status of the shipment.
    :param stockAllocatedShipmentReferenceNumber: (str) The stock allocated shipment reference number from property file
    :param productPickedShipmentReferenceNumber: (str) The product picked shipment reference number from property file
    :param referenceNumberActionEnumVal: (str) The reference number action enum value from property file
    :param waveOrPickCancelledShipmentReferenceNumber: (str) The wave pick cancelled shipment reference number from property file
    :param message_hdr_info: (dict) This contains all the details of messageIds, entity name etc.

    :returns:
        list of types.SimpleNamespace: A list of reference number update data as SimpleNamespace objects.
    """

    qualifier_name = "reference_number_update_data_shipment"

    reference_number_data = []
    ref_number = None

    # Determine the reference number based on the status code.
    if status_code == 'STOCK_ALLOCATED':
        ref_number = stockAllocatedShipmentReferenceNumber
    elif status_code == 'PRODUCT_PICKED':
        ref_number = productPickedShipmentReferenceNumber
    elif status_code == 'STOCK_ALLOCATION_UNSUCCESSFUL':
        ref_number = waveOrPickCancelledShipmentReferenceNumber

    try:
        system_reference_number_id = eval('reference_number_structure.systemReferenceNumberID')
    except Exception:
        system_reference_number_id = None

    try:
        reference_number_action_enum_val = eval('reference_number_structure.systemReferenceNumberID')
    except Exception:
        reference_number_action_enum_val = None

    # Create a SimpleNamespace object to represent the reference number data, so that this can be used in config file
    # to map the data
    reference_number_data.append(
        types.SimpleNamespace(
            mapped_data=types.SimpleNamespace(
                reference_number_entity_key=eval('shipment_document.systemShipmentID'),
                system_reference_number_id=system_reference_number_id
                if system_reference_number_id not in [None, ''] else None,
                reference_number_action_enum_val='AT_ADD'
                if reference_number_action_enum_val in [None, '']
                else referenceNumberActionEnumVal,
                reference_number=ref_number,
                reference_number_type_code='WM_SHIPMENT'
            )
        )
    )
    Logger.debug(f"{qualifier_name}, Update shipment references based on enriched shipment data body -> "
                 f"{reference_number_data}, {message_hdr_info}")
    return reference_number_data


def calculate_load_status(load_doc, ship_loc_code, stp_status):
    """
    Calculate the load status based on the provided parameters.

    :param load_doc: (dict) The load document containing stop information.
    :param ship_loc_code: (str) The shipping location code to match.
    :param stp_status: (str) The stop status to use.

    :returns:
        str: The calculated load status as a comma-separated string.
    """
    load_status_list = []
    for index, stop in enumerate(eval('load_doc.stop')):
        if index == 0 and eval('stop.shippingLocationCode') == ship_loc_code:
            load_status_list.append(stp_status)
        elif index == 0:
            try:
                # Check for a reference number with type 'WM_SHP_STS'.
                ref_num_struct = eval('stop.referenceNumberStructure')
                for ref in ref_num_struct:
                    if eval('ref.referenceNumberTypeCode') == 'WM_SHP_STS':
                        ref_num = eval('ref.referenceNumber') if eval(
                            'ref.referenceNumber') else ''
                        load_status_list.append(ref_num)
                    break
            except AttributeError:
                pass
        elif eval('stop.shippingLocationCode') == ship_loc_code:
            load_status_list.append(stp_status)
        else:
            try:
                # Check for a reference number with type 'WM_SHP_STS'.
                ref_num_struct = eval('stop.referenceNumberStructure')
                for ref in ref_num_struct:
                    if eval('ref.referenceNumberTypeCode') == 'WM_SHP_STS':
                        ref_num = eval('ref.referenceNumber') if eval(
                            'ref.referenceNumber') else ''
                        load_status_list.append(ref_num)
                    break
            except AttributeError as e:
                pass

    Logger.info(f"Calculated load status for shipment {load_status_list}")

    # Convert the load status list to a comma-separated string.
    if isinstance(load_status_list, str):
        return load_status_list
    elif isinstance(load_status_list, list):
        return ','.join(load_status_list)


def update_shipment_ref_based_on_enriched_shipment(*args):
    """
    Update shipment references based on enriched shipment data in the Transportation Management System (TMS).

    This function processes enriched shipment data, extracts relevant information, and updates shipment references
    in the TMS. It logs messages to provide information about the update process.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               won_shipment, enriched_shipment, status_code, stockAllocatedShipmentReferenceNumber,
               productPickedShipmentReferenceNumber, referenceNumberActionEnumVal, and message_hdr_info in that order.

    :return:
        list: A list of updated reference numbers for shipments.
    """
    # Extract arguments from *args
    won_shipment = args[0][0]
    enriched_shipment = args[0][1]
    status_code = args[0][2]
    tm_ship_ids = args[0][3]
    stockAllocatedShipmentReferenceNumber = args[0][4]
    productPickedShipmentReferenceNumber = args[0][5]
    waveOrPickCancelledShipmentReferenceNumber = args[0][6]
    referenceNumberActionEnumVal = args[0][7]

    message_hdr_info = args[0][-1]

    qualifier_name = 'update_shipment_ref_based_on_enriched_shipment'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    reference_number_list = []

    try:
        for ship_id in tm_ship_ids:
            for enrich_ship in enriched_shipment:
                if eval('enrich_ship.shipmentNumber') == ship_id:
                    shipment_document = enrich_ship

                    try:
                        reference_number_structure = get_reference_number_structure(shipment_document)
                    except Exception as e:
                        Logger.error(f"Not able to get the referenceNumberStructure for the shipment_document: {e}",
                                     message_hdr_info)
                        reference_number_structure = None

                    reference_number_update_data = \
                        reference_number_update_data_shipment(reference_number_structure, shipment_document,
                                                              status_code, stockAllocatedShipmentReferenceNumber,
                                                              productPickedShipmentReferenceNumber,
                                                              referenceNumberActionEnumVal,
                                                              waveOrPickCancelledShipmentReferenceNumber,
                                                              message_hdr_info)

                    if reference_number_update_data:
                        reference_number_list.append(reference_number_update_data)
                        Logger.debug(f'{qualifier_name}, Successfully completed forming the reference number update'
                                     f' data for shipment, {message_hdr_info}')

        Logger.debug(f'{qualifier_name}, End, {message_hdr_info}')
        Logger.debug(f"{qualifier_name}, Update shipment references based on enriched shipment data body -> "
                     f"{reference_number_list}, {message_hdr_info}")
        return reference_number_list
    except Exception as e:
        Logger.error(f'{qualifier_name},'
                     f'An error occurred while forming the reference_number_update_Data for shipment, '
                     f'{message_hdr_info}')


def update_shipment_ref_based_on_enriched_shipment_leg(*args):
    """
        Update shipment references based on enriched shipment leg data in the Transportation Management System (TMS).

        This function processes enriched shipment leg data, extracts relevant information, and updates shipment references
        in the TMS. It logs messages to provide information about the update process.

        :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
                   won_shipment, enriched_shipment, status_code, stockAllocatedShipmentReferenceNumber,
                   productPickedShipmentReferenceNumber, referenceNumberActionEnumVal, and message_hdr_info in that order.

        :return:
            list: A list of updated reference numbers for shipment legs.
    """
    won_shipment = args[0][0]
    enriched_shipment = args[0][1]
    status_code = args[0][2]
    tm_ship_ids = args[0][3]
    stockAllocatedShipmentReferenceNumber = args[0][4]
    productPickedShipmentReferenceNumber = args[0][5]
    referenceNumberActionEnumVal = args[0][6]

    message_hdr_info = args[0][-1]

    qualifier_name = 'update_shipment_ref_based_on_enriched_shipment_leg'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    reference_number_list = []

    try:
        for ship_id in tm_ship_ids:
            for enrich_ship in enriched_shipment:
                shipment_leg = eval('enrich_ship.shipmentLeg')
                for leg in shipment_leg:
                    if eval('leg.systemShipmentLegID') == ship_id and (eval('leg.systemShipmentLegID') is not None):
                        shipment_document = enrich_ship
                        try:
                            reference_number_structure = get_reference_number_structure(shipment_document)
                        except Exception as e:
                            Logger.error(f"{qualifier_name} Not able to get the referenceNumberStructure "
                                         f"for the shipment_document: {e}, {message_hdr_info}")
                            reference_number_structure = None

                        reference_number_update_data = reference_number_update_data_shipment(
                            reference_number_structure, shipment_document, status_code,
                            stockAllocatedShipmentReferenceNumber, productPickedShipmentReferenceNumber,
                            referenceNumberActionEnumVal, None, message_hdr_info)

                        if reference_number_update_data:
                            reference_number_list.append(reference_number_update_data)
                            Logger.debug(
                                f'{qualifier_name}, Successfully completed forming the reference number update'
                                f' data for shipment Leg, {message_hdr_info}')

        Logger.debug(f"{qualifier_name}, END {message_hdr_info}")
        Logger.debug(f"{qualifier_name}, Update shipment references based on enriched shipment leg data body -> "
                     f"{reference_number_list}, {message_hdr_info}")
        return reference_number_list
    except Exception:
        Logger.error(
            f"{qualifier_name}There is no reference number structure formed for ShipmentLeg information,"
            f" {message_hdr_info}")


def update_shipment_ref_based_on_enriched_merged_load(*args):
    """
    Update shipment references based on enriched merged load data in the Transportation Management System (TMS).

    This function processes enriched merged load data, extracts relevant information, and updates shipment references
    in the TMS. It logs messages to provide information about the update process.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               enriched_load, won_shipment, shipping_location_code, stockAllocatedShipmentReferenceNumber,
               productPickedShipmentReferenceNumber, referenceNumberActionEnumVal, stopStatusSuffix, and
               message_hdr_info in that order.

    :return:
        list: A list of updated reference numbers for the load.
    """
    updateLoadAndStopReferenceNumbers = args[0][0]
    enriched_load = args[0][1]
    won_shipment = args[0][2]
    shipping_location_code = args[0][3]
    tm_ship_ids = args[0][4]
    stockAllocatedShipmentReferenceNumber = args[0][5]
    productPickedShipmentReferenceNumber = args[0][6]
    referenceNumberActionEnumVal = args[0][7]
    stopStatusSuffix = args[0][8]

    message_hdr_info = args[0][-1]

    qualifier_name = 'update_shipment_ref_based_on_enriched_merged_load'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    load_document = next((eval('load')
                          for load in enriched_load
                          for shipment in won_shipment
                          if eval('load.systemLoadID') == eval(
        'shipment.shipmentTransportationInformation.transportLoadId')
                          ), None)
    try:
        load_reference_number_id = next((eval('ref.systemReferenceNumberID')
                                         for ref in eval('load_document.referenceNumberStructure')
                                         if eval('ref.referenceNumberTypeCode') == 'WM_WH_STS_'
                                         ), None)
    except Exception:
        load_reference_number_id = None

    stop_status = shipping_location_code + '-' + stopStatusSuffix

    load_status = calculate_load_status(load_document, shipping_location_code, stop_status)
    Logger.info(f"{qualifier_name}, The load calculated is: {load_status}",message_hdr_info)

    reference_number_update_data = []

    # Create a SimpleNamespace object to represent the reference number data, so that this can be used in config file
    # to map the data
    if eval('load_document.systemLoadID') and (referenceNumberActionEnumVal == 'AT_UPDATE' or load_reference_number_id):
        reference_number_update_data.append(types.SimpleNamespace(
            load_mapped_data=types.SimpleNamespace(
                reference_number_entity_key=eval('load_document.systemLoadID'),
                system_reference_number_id=load_reference_number_id,
                reference_number_action_enum_val='AT_ADD' if load_reference_number_id in [None, '']
                else referenceNumberActionEnumVal,
                reference_number=load_status
            )
        )
        )
        Logger.debug(f'{qualifier_name}, Successfully completed forming the reference number update data for load'
                     f', {message_hdr_info}')
        Logger.debug(f"{qualifier_name}, Update shipment references based on enriched load data body -> "
                     f"{reference_number_update_data}, {message_hdr_info}")
    else:
        reference_number_update_data = []
    Logger.debug(f'{qualifier_name}, END, {message_hdr_info}')
    return reference_number_update_data


def update_shipment_ref_based_on_enriched_merged_stop(*args):
    """
    Update reference numbers for a stop based on enriched load data and related information.

    This function updates the reference numbers for a stop in a warehousingOutboundNotification (warehousingOutboundNotification) based on
    enriched load data and other relevant information.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               enriched_load, won_shipment, shipping_location_code, stockAllocatedShipmentReferenceNumber,
               productPickedShipmentReferenceNumber, referenceNumberActionEnumVal, stopStatusSuffix, and
               message_hdr_info in that order.

    :return:
        list: A list containing the updated reference number data for the stop.

    """

    enriched_load = args[0][0]
    won_shipment = args[0][1]
    shipping_location_code = args[0][2]
    tm_ship_ids = args[0][3]
    stockAllocatedShipmentReferenceNumber = args[0][4]
    productPickedShipmentReferenceNumber = args[0][5]
    referenceNumberActionEnumVal = args[0][6]
    stopStatusSuffix = args[0][7]

    message_hdr_info = args[0][-1]

    qualifier_name = 'update_shipment_ref_based_on_enriched_merged_stop'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    reference_number_update_data = []

    load_document = next((eval('load')
                          for load in enriched_load
                          for shipment in won_shipment
                          if eval('load.systemLoadID') == eval(
        'shipment.shipmentTransportationInformation.transportLoadId')), None)
    stop_status = shipping_location_code + '-' + stopStatusSuffix

    stop_document = next((eval('stop')
                          for stop in eval('load_document.stop')
                          if eval('stop.shippingLocationCode') == shipping_location_code), None)

    try:
        stop_reference_number_id = next((eval('ref.systemReferenceNumberID')
                                         for ref in eval('stop_document.referenceNumberStructure')
                                         if eval('ref.referenceNumberTypeCode') == 'WM_SHP_STS'), None)
    except Exception:
        stop_reference_number_id = None
    # Create a SimpleNamespace object to represent the reference number data, so that this can be used in config file
    # to map the data
    if eval('stop_document.systemStopID') or stop_reference_number_id:
        reference_number_update_data.append(types.SimpleNamespace(
            stop_mapped_data=types.SimpleNamespace(
                reference_number_entity_key=eval('stop_document.systemStopID'),
                system_reference_number_id=stop_reference_number_id,
                reference_number_action_enum_val='AT_ADD' if stop_reference_number_id in [None, '', []]
                else referenceNumberActionEnumVal,
                reference_number=stop_status
            )
        )
        )
    Logger.debug(f'{qualifier_name}, Successfully completed forming the reference number update data for stop'
                 f', {message_hdr_info}')
    Logger.debug(f"{qualifier_name}, Update shipment references based on enriched load stop body -> "
                 f"{reference_number_update_data}, {message_hdr_info}")
    Logger.debug(f'{qualifier_name}, END, {message_hdr_info}')

    return reference_number_update_data


def get_waved_picked_shipments_mapped_data(enriched_load, shipment_reference_number,
                                           ShipmentReferenceNumber, shipping_location_code):
    """
    Get the number of waved shipments based on enriched load data, shipping location code, and reference number.

    This function will give the shipment leg waved or picked.

    The data is taken from shipment reference number formed from reference number update config file.
    We are matching the reference type code to WM_SHIPMENT, reference number with the incoming ShipmentReferenceNumber,
    and shippingLocationCode with the incoming shipping location code.
    This would give the respective shipmentLeg Waved or Picked
    """
    ship_leg_waved_picked = [[next(
        eval('leg')
        for load in enriched_load
        for leg in eval('load.shipmentLeg')
        for ship_ref in shipment_reference_number[0]
        if (
                eval('ship_ref.mapped_data.reference_number_type_code') == 'WM_SHIPMENT'
                and eval('ship_ref.mapped_data.reference_number') == ShipmentReferenceNumber
                and eval('leg.shipment.shipFromLocationCode') == shipping_location_code
        ))
    ]]
    return ship_leg_waved_picked


def get_waved_picked_shipments(enriched_load, ShipmentReferenceNumber, shipping_location_code):
    """
    Get the number of waved shipments based on enriched load data, shipping location code, and reference number.

    This function will give the shipment leg waved or picked.

    We are taking the reference number structure from the enriched load message.
    We are matching the reference type code to WM_SHIPMENT, reference number with the incoming ShipmentReferenceNumber,
    and shippingLocationCode with the incoming shipping location code.
    This would give the respective shipmentLeg Waved or Picked
    """
    ship_leg_waved_picked = [[
        eval('leg')
        for load in enriched_load
        for leg in eval('load.shipmentLeg')
        for ref in eval('leg.shipment.referenceNumberStructure')
        if (
                eval('ref.referenceNumberTypeCode') == 'WM_SHIPMENT'
                and eval('ref.referenceNumber') == ShipmentReferenceNumber
                and eval('leg.shipment.shipFromLocationCode') == shipping_location_code
        )
    ]]
    return ship_leg_waved_picked


def get_number_of_waved_shipments(*args):
    """
    Get the number of waved shipments based on enriched load data, shipping location code, and reference number.

    This function searches for shipments that match the specified criteria and counts them.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               enriched_load, shipping_location_code, and stockAllocatedShipmentReferenceNumber in that order.

    :return:
        int: The number of waved shipments that match the criteria.
    """
    enriched_load = args[0][0]
    shipping_location_code = args[0][1]
    shipment_reference_number = args[0][2]
    won_status = args[0][3]
    tm_shipment_ids = args[0][4]
    stockAllocatedShipmentReferenceNumber = args[0][5]

    message_hdr_info = args[0][-1]

    qualifier_name = "get_number_of_waved_shipments"
    Logger.debug(f"{qualifier_name}, Running, {message_hdr_info}")

    # added this functionality because, right now Neo doesn't have the functionality to update the reference number
    # while transformation and enriching the shipment call again.
    # Added a fix to fetch the reference number from the formed payload of reference number updates.
    try:
        ship_leg_waved = get_waved_picked_shipments_mapped_data(enriched_load, shipment_reference_number,
                                                                stockAllocatedShipmentReferenceNumber,
                                                                shipping_location_code)
    except Exception:
        ship_leg_waved = [[]]

    if ship_leg_waved == [[]]:
        try:
            ship_leg_waved = get_waved_picked_shipments(enriched_load,
                                                        stockAllocatedShipmentReferenceNumber,
                                                        shipping_location_code)
        except Exception:
            ship_leg_waved = [[]]

    if ship_leg_waved[0]:
        waved_shipments = {'shipmentLeg': ship_leg_waved[0]}
    else:
        waved_shipments = {}
    if waved_shipments:
        ship_legs = waved_shipments.get('shipmentLeg', [])
        ship_leg = ship_legs[0]
        if eval('ship_leg.systemShipmentLegID') in [None, '']:
            waved_value = 0
        else:
            waved_value = len(ship_legs)
    else:
        waved_value = 0

    # added a check on the status of WON_STATUS, and compare the shipment_refs and shipment legs
    # in the payload and based on that calculate the waved_value
    if won_status == 'STOCK_ALLOCATION_UNSUCCESSFUL':
        shipment_refs = len(shipment_reference_number)
        shipment_leg = eval('enriched_load[0].shipmentLeg')

        # to keep track of the shipments for that particular warehouse
        ship_leg_count = 0

        for ship in shipment_leg:
            if eval('ship.shipFromLocationCode') == shipping_location_code:
                ship_leg_count += 1

        if len(tm_shipment_ids) == ship_leg_count:
            # when the consolidation is on, there will all the shipments in the same won message, and it would be
            # a direct count of the shipment reference number formed from the payload of reference number update config
            # file and to the shipment leg count for that particular load and shipping location code.
            # if the count is same, then we would update the stop planning status to default, else we would not update
            # it.
            waved_value = 0 if shipment_refs == ship_leg_count else waved_value
        else:
            # when the consolidation is off, there will be single shipment per won message. In order to
            # get the count of wave cancelled shipments separately, we are performing the below functionality.
            # where we take the count of reference numbers of Wave Cancelled for a load and count of Wave Cancelled
            # per payload formed in reference number update config file and if both the count matches with
            # shipment leg for that Load and Shipping Location, then we would update the stop planning status to
            # default. Else it would not get changed.

            # Please Do not touch the below logic, as it would complicate things to STOCK_ALLOCATION_UNSUCCESSFUL
            shipment_leg_wave_cancelled_from_ref_num = get_waved_picked_shipments(enriched_load,
                                                                                  "Wave Cancelled",
                                                                                  shipping_location_code)

            shipment_leg_wave_cancelled_from_mapped_ref_num = \
                get_waved_picked_shipments_mapped_data(enriched_load, shipment_reference_number,
                                                       "Wave Cancelled", shipping_location_code)
            if len(shipment_leg_wave_cancelled_from_ref_num[0]) + \
                    len(shipment_leg_wave_cancelled_from_mapped_ref_num[0]) == ship_leg_count:
                waved_value = 0

    Logger.debug(f"The total number of waved shipments are: {waved_value}", message_hdr_info)
    Logger.debug(f"{qualifier_name}, END, {message_hdr_info}")

    return waved_value


def get_number_of_picked_shipments(*args):
    """
    Get the number of picked shipments based on enriched load data, shipping location code, and reference number.

    This function searches for shipments that match the specified criteria and counts them.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               enriched_load, shipping_location_code, and productPickedShipmentReferenceNumber in that order.

    :return:
        int: The number of picked shipments that match the criteria.
    """
    enriched_load = args[0][0]
    shipping_location_code = args[0][1]
    shipment_reference_number = args[0][2]
    won_status = args[0][3]
    tm_shipment_ids = args[0][4]
    productPickedShipmentReferenceNumber = args[0][5]

    message_hdr_info = args[0][-1]

    qualifier_name = "get_number_of_picked_shipments"
    Logger.debug(f"{qualifier_name}, Running, {message_hdr_info}")

    # added this functionality because, right now Neo doesn't have the functionality to update the reference number
    # while transformation and enriching the shipment call again.
    # Added a fix to fetch the reference number from the formed payload of reference number updates.
    try:
        ship_leg_picked = get_waved_picked_shipments_mapped_data(enriched_load, shipment_reference_number,
                                                                 productPickedShipmentReferenceNumber,
                                                                 shipping_location_code)
    except Exception:
        ship_leg_picked = [[]]

    if ship_leg_picked == [[]]:
        try:
            ship_leg_picked = get_waved_picked_shipments(enriched_load,
                                                         productPickedShipmentReferenceNumber,
                                                         shipping_location_code)
        except Exception:
            ship_leg_picked = [[]]

    if ship_leg_picked[0]:
        picked_shipments = {'shipmentLeg': ship_leg_picked[0]}
    else:
        picked_shipments = {}

    if picked_shipments:
        ship_legs = picked_shipments.get('shipmentLeg', [])
        ship_leg = ship_legs[0]
        if eval('ship_leg.systemShipmentLegID') in [None, '']:
            picked_value = 0
        else:
            picked_value = len(ship_legs)
    else:
        picked_value = 0

    # added a check on the status of WON_STATUS, and compare the shipment_refs and shipment legs
    # in the payload and based on that calculate the picked_value
    if won_status == 'STOCK_ALLOCATION_UNSUCCESSFUL':
        shipment_refs = len(shipment_reference_number)
        shipment_leg = eval('enriched_load[0].shipmentLeg')

        # to keep track of the shipments for that particular warehouse
        ship_leg_count = 0

        for ship in shipment_leg:
            if eval('ship.shipFromLocationCode') == shipping_location_code:
                ship_leg_count += 1

        if len(tm_shipment_ids) == ship_leg_count:
            # when the consolidation is on, there will all the shipments in the same won message, and it would be
            # a direct count of the shipment reference number formed from the payload of reference number update config
            # file and to the shipment leg count for that particular load and shipping location code.
            # if the count is same, then we would update the stop planning status to default, else we would not update
            # it.
            picked_value = 0 if shipment_refs == ship_leg_count else picked_value
        else:
            # when the consolidation is off, there will be single shipment per won message. In order to
            # get the count of wave cancelled shipments separately, we are performing the below functionality.
            # where we take the count of reference numbers of Wave Cancelled for a load and count of Wave Cancelled
            # per payload formed in reference number update config file and if both the count matches with
            # shipment leg for that Load and Shipping Location, then we would update the stop planning status to
            # default. Else it would not get changed.

            # Please Do not touch the below logic, as it would complicate things to STOCK_ALLOCATION_UNSUCCESSFUL
            shipment_leg_wave_cancelled_from_ref_num = get_waved_picked_shipments(enriched_load,
                                                                                  "Wave Cancelled",
                                                                                  shipping_location_code)

            shipment_leg_wave_cancelled_from_mapped_ref_num = get_waved_picked_shipments_mapped_data(
                    enriched_load, shipment_reference_number, "Wave Cancelled", shipping_location_code)

            if len(shipment_leg_wave_cancelled_from_ref_num[0]) + len(
                    shipment_leg_wave_cancelled_from_mapped_ref_num[0]) == ship_leg_count:
                picked_value = 0

    Logger.debug(f"The total number of picked shipments are: {picked_value}, {message_hdr_info}")
    Logger.debug(f"{qualifier_name}, END, {message_hdr_info}")

    return picked_value


def get_stop_planning_status(*args):
    """
    Get the stop planning status for a specific shipping location in enriched load data.

    This function searches for the stop planning status associated with the specified shipping location.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               enriched_load and shipping_location_code in that order.

    :return:
        str or None: The stop planning status for the specified shipping location, or None if not found.
    """

    enriched_load = args[0][0]
    shipping_location_code = args[0][1]

    message_hdr_info = args[0][-1]

    qualifier_name = "get_stop_planning_status"
    Logger.debug(f"{qualifier_name}, Running, {message_hdr_info}")

    stop_planning_status = None
    for load in enriched_load:
        stop_planning_status = next((
            eval('stop.stopPlanningStatusEnumVal')
            for stop in eval('load.stop')
            if eval('stop.shippingLocationCode') == shipping_location_code
        ), None)

    Logger.debug(f"Stop Planning status from stop enriched_load is {stop_planning_status}", message_hdr_info)
    Logger.debug(f"{qualifier_name}, END, {message_hdr_info}")
    return stop_planning_status


def get_stop_planning_status_enum_val(*args):
    """
    Get the stop planning status enum value based on various conditions and input values.

    This function determines the appropriate stop planning status enum value based on the provided input values.

    :param args: A variable-length argument list. It is assumed that the arguments are in the following order:
               - status_code: The status code.
               - waved_value: The number of waved shipments.
               - picked_value: The number of picked shipments.
               - stop_planning_status: The current stop planning status.
               - stopPlanningStatusForWave: The stop planning status for wave conditions.
               - stopPlanningStatusForProductPicked: The stop planning status for product picked conditions.
               - stopPlanningStatusForWaveCancelled: The stop planning status for wave cancelled conditions.

    :return:
        str or None: The calculated stop planning status enum value, or None if not applicable.

    """
    status_code = args[0][0]
    waved_value = args[0][1]
    picked_value = args[0][2]
    stop_planning_status = args[0][3]
    stopPlanningStatusForWave = args[0][4]
    stopPlanningStatusForProductPicked = args[0][5]
    stopPlanningStatusForWaveCancelled = args[0][6]

    message_hdr_info = args[0][-1]

    qualifier_name = "get_stop_planning_status_enum_val"
    Logger.debug(f"{qualifier_name}, Running ", message_hdr_info)

    # Logic checks for STOCK_ALLOCATION_UNSUCCESSFUL, STOCK_ALLOCATED, PRODUCT_PICKED

    if status_code == 'STOCK_ALLOCATION_UNSUCCESSFUL' and \
            picked_value == 0 and waved_value > 0 \
            and stop_planning_status != 'STPPLNG_STAT_PICKED':
        stop_planning_status_enum_val = stopPlanningStatusForWave
    elif status_code == 'STOCK_ALLOCATION_UNSUCCESSFUL' and \
            waved_value == 0 and picked_value == 0 and stop_planning_status != 'STPPLNG_STAT_DEFAULT':
        stop_planning_status_enum_val = stopPlanningStatusForWaveCancelled
    elif status_code == 'STOCK_ALLOCATED' and \
            waved_value > 0 and picked_value < 1 and \
            stop_planning_status != 'STPPLNG_STAT_PICKED':
        stop_planning_status_enum_val = stopPlanningStatusForWave
    elif status_code == 'PRODUCT_PICKED' and stop_planning_status != 'STPPLNG_STAT_LOCKED':
        stop_planning_status_enum_val = stopPlanningStatusForProductPicked
    else:
        stop_planning_status_enum_val = None

    Logger.debug(f"The Stop Planning Status Enum Val is {stop_planning_status_enum_val}")
    Logger.debug(f"{qualifier_name}, End ", message_hdr_info)
    return stop_planning_status_enum_val


def get_tms_shipment_ids(*args):
    """
    Get the TMS shipment IDs from a list of transactional references.

    This function extracts the TMS shipment IDs from a list of transactional references.

    :param args: A variable-length argument list. It is assumed that the first argument (*args[0]) contains the
               transactional_refs, which is a list of references.

    :return:
        list: A list of TMS shipment IDs extracted from the transactional references.
    """
    transactional_refs = args[0][0]

    tms_shipment_ids = []
    for ref in transactional_refs:
        try:
            # Picking only for SRN typecode, as we need to update only SRN type transactionalReferenceTypeCode
            if eval('ref.transactionalReferenceTypeCode') == 'SRN':
                tms_shipment_ids.append(eval('ref.entityId'))
        except Exception:
            pass

    return tms_shipment_ids


def update_shipment_ref_based_on_enriched_shipment_leg_stock_cancel(*args):
    """
    Update shipment leg reference based on enriched shipment data for stock cancellation.

    This function updates the shipment leg reference based on enriched shipment data for stock cancellation scenarios.

    :param args: A variable-length argument list. It is assumed that the arguments are in the following order:
               - won_shipment: The original shipment data.
               - enriched_shipment: The enriched shipment data.
               - status_code: The status code indicating stock cancellation.
               - tm_ship_ids: The list of TMS shipment IDs associated with the shipment legs.
               - waveOrPickCancelledShipmentReferenceNumber: The reference number for stock cancellation.
               - referenceNumberActionEnumVal: The action enum value for reference number update.

    :return:
        list: A list of reference number update data for the shipment legs.

    """
    won_shipment = args[0][0]
    enriched_shipment = args[0][1]
    status_code = args[0][2]
    tm_ship_ids = args[0][3]
    waveOrPickCancelledShipmentReferenceNumber = args[0][4]
    referenceNumberActionEnumVal = args[0][5]

    message_hdr_info = args[0][-1]

    qualifier_name = 'update_shipment_ref_based_on_enriched_shipment_leg_stock_cancel'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    reference_number_list = []

    try:
        for ship_id in tm_ship_ids:
            for enrich_ship in enriched_shipment:
                for shipment_leg in eval('enrich_ship.shipmentLeg'):
                    if eval('shipment_leg.systemShipmentLegID') == ship_id:
                        shipment_document = enrich_ship

                        try:
                            reference_number_structure = get_reference_number_structure(shipment_document)
                        except Exception as e:
                            Logger.error(f"Not able to get the referenceNumberStructure for the shipment_document: {e}")
                            reference_number_structure = None

                        reference_number_update_data = \
                            reference_number_update_data_shipment(reference_number_structure, shipment_document,
                                                                  status_code, None, None,
                                                                  referenceNumberActionEnumVal,
                                                                  waveOrPickCancelledShipmentReferenceNumber,
                                                                  message_hdr_info)

                        if reference_number_update_data:
                            reference_number_list.append(reference_number_update_data)
                            Logger.debug(f'{qualifier_name}, Successfully completed forming the reference number '
                                         f'update data for shipment leg stock cancel, {message_hdr_info}')

        Logger.debug(
            f"{qualifier_name}, Update shipment references based on enriched shipment leg stock cancel body -> "
            f"{reference_number_list}, {message_hdr_info}")
        Logger.debug(f'{qualifier_name}, End, {message_hdr_info}')
        return reference_number_list
    except Exception as e:
        Logger.error(f'{qualifier_name},'
                     f'An error occurred while forming the reference_number_update_Data for shipment Leg')


def set_updateLoadAndStopReferenceNumbers(*args):
    """
    Set flag to update load and stop reference numbers.

    This function sets a flag to determine whether load and stop reference numbers should be updated.

    :param args: A variable-length argument list. It is assumed that the arguments are in the following order:
               - flag: An integer flag value (0 or 1) indicating whether reference numbers should be updated.

    :return:
        bool: True if the flag is 0 (indicating no update needed), False otherwise.

    """
    if args[0][0] == 0:
        return True
    else:
        return False


def get_count_of_unwaved_shipments_at_stop(*args):
    """
    Check if a shipment is unwaved at a stop.

    This function compares the 'shipFromLocationCode' and 'partyId' to determine if a shipment should be considered
    as unwaved at a stop.

    :param args: A variable-length argument list. It is assumed that the arguments are in the following order:
               - shipFromLocationCode: The location code.
               - partyId: The party ID.

    :return:
        int: 1 if the shipment is unwaved (location code equals party ID), else 0.
    """
    shipFromLocationCode = args[0][0]
    partyId = args[0][1]

    return 1 if shipFromLocationCode == partyId else 0


def extract_variable_containers(*args):
    """
    Extracts the containers

    This function will extract containers from the enriched shipment details based on the shipment number and
    referenceNumberTypeCode

    :param args: A variable length argument list. It is assumed that the arguments are in the following order:
                - enriched_shipment: The data of shipment after making a shipment enrichment call
                - shipment_ids: list of shipment ids

    :return:
        list of matched containers

    """
    enriched_shipment = args[0][0]
    shipment_ids = args[0][1]

    message_hdr_info = args[0][-1]

    qualifier_name = 'extract_variable_containers'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')
    result = []

    if enriched_shipment:

        try:
            for shipment in enriched_shipment:
                shipment_number = getattr(shipment, "shipmentNumber")

                try:
                    shipment_container = getattr(shipment, "container")

                    if shipment_container:
                        for cont in shipment_container:

                            reference_number_structure = getattr(cont, "referenceNumberStructure")
                            for ref in reference_number_structure:

                                # check if the referenceNumberTypeCode is "WM_ORDLIN_"
                                if getattr(ref, "referenceNumberTypeCode") == "WM_ORDLIN_":
                                    key = f"{shipment_number}_{getattr(cont, 'systemContainerID')}"
                                    result.append({key: cont})
                except Exception as ex:
                    Logger.error(f'{qualifier_name},'
                                 f'An error occurred while forming the containers -> {ex}')
        except Exception as ex:
            Logger.error(f'{qualifier_name},'
                         f'An error occurred while looping over enriched shipment -> {ex}')

        Logger.debug(
            f"{qualifier_name}, Container variable -> "
            f"{result}, {message_hdr_info}")
        Logger.debug(f"{qualifier_name}, End, {message_hdr_info}")
        return result if result else None


def namespace_to_dict(namespace_):
    """
    This function would convert the namespace type value to dictionary.
    This function would return compressed_message only, if the data is compressed.
    input data: namespace(a='1234', b='2345', namespace(c='23456'))
    output data: {a: '1234',
                  b: '2345',
                  {c: '23456'}}
    """
    if isinstance(namespace_, types.SimpleNamespace):
        namespace_ = vars(namespace_)
    if isinstance(namespace_, dict):
        return {key: namespace_to_dict(value) for key, value in namespace_.items()}
    elif isinstance(namespace_, (list, tuple)):
        return [namespace_to_dict(item) for item in namespace_]
    else:
        return namespace_


def container_map(shipment_document, vars_containers, shipment,
                  tmVolumeUOM, tmWeightUOM, message_hdr_info):
    """
    Helper function to form the container mapping info

    This function forms the data of containers based on shipment number and won line item number.

    :param shipment_document: A current shipment number shipment segment
    :param vars_containers: extracted containers
    :param shipment: won shipment
    :param tmVolumeUOM: tms volume property
    :param tmWeightUOM: tms weight property
    :param message_hdr_info: Message header info details
    """
    containers = []
    shipment_container_document_without_transport_order = {}
    qualifier_name = 'container_map'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    shipment_mode = shipment_document.get('shipmentEntryModeEnumVal')
    for ship_item in shipment.get("shipmentItem", []):
        transactional_reference = ship_item.get('transactionalReference', [])
        for tref in transactional_reference:

            # fetch the won_line_item_number based on SRN referenceTypeCode
            won_line_item_number = tref.get("lineItemNumber") \
                if tref.get('transactionalReferenceTypeCode') == 'SRN' else None

            cont_key = f"{shipment_document.get('shipmentNumber')}_{won_line_item_number}"

            if won_line_item_number is not None:
                for cont in vars_containers:
                    if cont.get(cont_key):
                        value = cont.get(cont_key)

                        inputQty = ship_item.get("plannedDespatchQuantity", {}).get("value")
                        if inputQty != 0:

                            # As per mule codebase, we are fetching the first segment details of logisticUnit
                            inputWeightUOM = ship_item.get('logisticUnit', [{}])[0].get('grossWeight', {}).get(
                                "measurementUnitCode")

                            inputVolumeUOM = ship_item.get('logisticUnit', [{}])[0].get('unitMeasurement', [{}])[0].get(
                                'measurementValue', {}).get('measurementUnitCode')

                            inputTotalGrossWeightValue = ship_item.get('logisticUnit', [{}])[0].get('grossWeight', {}).get(
                                "value")

                            inputTotalGrossVolume = ship_item.get('logisticUnit', [{}])[0].get(
                                'unitMeasurement', [{}])[0].get('measurementValue', {}).get('value')

                            try:
                                convertedWeight = operation_utils.OperationUtils.getUomConversion("Weight", inputWeightUOM,
                                                                                                  tmWeightUOM,
                                                                                                  inputTotalGrossWeightValue)
                            except Exception as ex:
                                Logger.error(f"{qualifier_name} An error occurred while performing UOM Conversion -> {ex}")
                                convertedWeight = 0.0

                            inputUnitWeight = round(float(convertedWeight) / float(inputQty), 4)

                            try:
                                convertedVolume = operation_utils.OperationUtils.getUomConversion("Volume", inputVolumeUOM,
                                                                                                  tmVolumeUOM,
                                                                                                  inputTotalGrossVolume)
                            except Exception as ex:
                                Logger.error(f"{qualifier_name} An error occurred while performing UOM Conversion -> {ex}")
                                convertedVolume = 0.0

                            containerUnitVolume = round(float(convertedVolume) / float(inputQty), 4)

                            value.update({"inputUnitWeight": inputUnitWeight, "containerUnitVolume": containerUnitVolume,
                                          "inputQty": inputQty,
                                          "weightByFreightClass": [
                                              {"freightClassNominalWeight": inputUnitWeight,
                                               "freightClassCode": value.get("weightByFreightClass", [{}])[0].get(
                                                   "freightClassCode")}],
                                          "ignoreShipmentItemsFlag": True
                                          if shipment_mode != "ILD_SUMMARY_MANDATORY" else
                                          None,
                                          "ignoreWeightByFreightClassFlag": False
                                          if shipment_mode != "ILD_SUMMARY_MANDATORY" else
                                          None})
                        containers.append(value)

    containers_ = [item for item in containers if item is not None]

    if containers_:
        shipment_container_document_without_transport_order = {"currentContainer": containers_}
        Logger.debug(f'{qualifier_name}, End, {message_hdr_info}')
        return shipment_container_document_without_transport_order
    return None


def form_shipment_container_document_without_transport_order(*args):
    """
    This function would give the shipment container document without transport order message

    :param args: A variable length argument list. It is assumed that the arguments are in the following order:
                - tms_shipment_ids: a list of shipment ids found at header level of transactionalReference segment
                - enriched_shipment: The data of shipment after making a shipment enrichment call
                - vars_containers: the extracted containers
                - statusCode: WON status code
                - won_shipment: the shipment details of incoming WON Message
                - tmVolumeUOM: volume property
                - tmWeightUOM: weight property

    :return: deserialised_message of shipment_container_document_without_transport_order

    """
    tms_shipment_ids = args[0][0]
    enriched_shipment = args[0][1]
    vars_containers = args[0][2]
    statusCode = args[0][3]
    won_shipment = args[0][4]
    tmVolumeUOM = args[0][5]
    tmWeightUOM = args[0][6]

    message_hdr_info = args[0][-1]

    qualifier_name = 'form_shipment_container_document_without_transport_order'
    Logger.debug(f'{qualifier_name}, Running, {message_hdr_info}')

    enriched_shipment = namespace_to_dict(enriched_shipment) if enriched_shipment else None
    vars_containers = namespace_to_dict(vars_containers) if vars_containers else None
    won_shipment = namespace_to_dict(won_shipment) if won_shipment else None
    container_document_without_transport_order = {}
    shipment_container_document_without_transport_order_ = []
    if tms_shipment_ids and enriched_shipment and vars_containers and won_shipment:

        try:
            for shipment in won_shipment:
                for tref in shipment.get('transactionalReference', []):
                    current_shipment_id = tref["entityId"]
                    if tref["transactionalReferenceTypeCode"] != "ON":
                        shipment_document = next(
                            (shipment
                             for shipment in enriched_shipment
                             if shipment.get('shipmentNumber') == current_shipment_id
                             ), {})

                        if shipment_document:
                            shipment_operational_status = shipment_document["currentShipmentOperationalStatusEnumVal"]

                            if shipment_operational_status != "S_CONFIRMED" and statusCode != "STOCK_ALLOCATED":

                                # call to container map function which would map the containers without
                                # transport order information
                                container_document_without_transport_order = container_map(shipment_document,
                                                                                           vars_containers,
                                                                                           shipment,
                                                                                           tmVolumeUOM, tmWeightUOM,
                                                                                           message_hdr_info)
                        shipment_container_document_without_transport_order_.append(
                            container_document_without_transport_order)

        except Exception as ex:
            Logger.error(f'{qualifier_name},'
                         f'An error occurred while forming shipment container document without transport order -> {ex}')
        if shipment_container_document_without_transport_order_:
            try:
                serialised_message = json.dumps(shipment_container_document_without_transport_order_)
                deserialised_message = json.loads(serialised_message, object_hook=lambda d: types.SimpleNamespace(**d))
                Logger.debug(f"Body of shipment container details -> {deserialised_message}")
                Logger.debug(f'{qualifier_name}, End, {message_hdr_info}')
                return deserialised_message
            except Exception as e:
                Logger.error(f"{qualifier_name}, An error occurred while serialising or deserialising the message-> "
                             f"{e}")
                return None
    else:
        return None


"""############################## WON functions END ##########################################"""
