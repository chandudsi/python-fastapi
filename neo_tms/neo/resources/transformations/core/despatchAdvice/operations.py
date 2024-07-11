from neo.log import Logger as logger
from neo.process.transformation.utils.operation_util import OperationUtils
from collections import defaultdict

"""
Get shipment leg information in process_load_status_update
flow in Tranformation DA
"""
def get_shipment_leg(argument_list,payload, global_variable):
    try:
        shipment_list = eval('payload.enrichment.shipment')
        shipment_leg = []
        for shipment in shipment_list:
            shipment_leg.append(*eval('shipment.shipmentLeg'))
        return shipment_leg
    except Exception as e:
        raise Exception(f'Execption occurs while fetch shipment leg from shipment data'
                        f'for Despatch Advice in TM entity in get_shipment_leg function'
                        f'{e}')


"""
Get list of stop sequence number in DA 
process_load_update_status
"""
def form_stop_look_up(argument_list,payload, global_variable):
    try:
        shipping_location_system_legId = {}
        stops = eval('payload.enrichment.load[0].stop')
        loads = eval('payload.enrichment.load')
        for load in loads:
            stops = eval('load.stop')
            load_id = eval('load.systemLoadID')
            for stop in stops:
                try:
                    ShippingLocationCode = eval('stop.shippingLocationCode')
                    StopSequenceNumber = eval('stop.stopSequenceNumber')
                    shipping_location_system_legId[f'{ShippingLocationCode}_{load_id}'] = StopSequenceNumber
                except:
                    pass
        return shipping_location_system_legId
    except Exception as e:
        raise Exception(f'Expection occurs while executing form_stop_look_up functions'
                        f'{e}')



"""
Fetch unique load id in DA message
"""
def get_unique_load_despatch_advice(argument_list, payload, global_variable):
    try:
        unique_load_id = {}
        da_document = []
        shipping_location = {}
        for item in eval('payload.despatchAdvice'):
            if eval('item.documentActionCode') != 'DELETE' and eval(
                    'item.despatchAdviceTransportInformation.transportLoadId') is not None \
                    and unique_load_id.get(eval('item.despatchAdviceTransportInformation.transportLoadId'),
                                           None) is None \
                    and shipping_location.get(eval('item.shipFrom.primaryId'), None) is None:
                da_document.append(item)
                shipping_location.update(
                    {eval('item.shipFrom.primaryId'): f'shipping location code {eval("item.shipFrom.primaryId")}'})

        return da_document if len(da_document) else None
    except Exception as e:
        logger.debug(f'Exception occurs while fetch unique load for Despatch Advice'
                     f'in TM flow {e}')
        return None



"""
Get current load from despatch advice message
"""
def get_current_load(load_id, payload, global_variable):
    load = eval('payload.enrichment.load')
    if load is None:
        return None
    for load_doc in load:
        if eval('load_doc.systemLoadID') == load_id[0]:
            return load_doc
    return None



"""
Get shipping location document for DA flow
"""
def get_shipping_location_information_document(shipping_id, payload, global_variable):
    try:
        shipping_location = eval('payload.enrichment.shippingLocation')
        if shipping_location is None:
            return None
        for shipping_doc in shipping_location:
            if eval('shipping_doc.shippingLocationCode') == shipping_id[0]:
                return shipping_doc

        return None
    except Exception as e:
        logger.debug(f'Expection occurs while executing shipping_location_information_document'
                        f'for Despatch Advice in TM {e}')
        return None



"""
Get mbol value based on ltl and ftl logic
"""
def get_mbol_value(argument_list,payload, global_variable):
    current_da = argument_list[0]
    try:
        logistic_service_code = eval('current_da.load.logisticServiceRequirementCode')

        if logistic_service_code == "2":
            return eval('current_da.load.billOfLadingNumber.entityId')
        elif logistic_service_code == "3":
            return eval('current_da.despatchAdviceTransportInformation.billOfLadingNumber.entityId')
        else:
            return None
    except Exception as e:
        logger.debug(f'Exception occurs while getting mbol value in '
                        f'Despatch Advice in TM flow {e}')
        return None



"""
Get tractor number information
"""
def get_tractor_number(argument_list,payload, global_variable):
    try:
        current_da = argument_list[0]
        if eval('current_da.documentActionCode') != 'DELETE':
            try:
                return eval('current_da.despatchAdviceTransportInformation.transportMeansID')
            except:
                return None
        return None
    except Exception as e:
        logger.debug(f'Expection occurs while getting tractor number for DespatchAdvice'
                        f'in TM flow {e}')
        return None



"""
Get trailer number info 
"""
def get_trailer_number(argument_list, payload, global_variable):
    try:
        current_da = argument_list[0]
        if eval('current_da.documentActionCode') != 'DELETE':
            try:
                return eval('current_da.load.transportEquipment.assetId[0].primaryId')
            except:
                return None
        return None
    except Exception as e:
        logger.debug(f'Exception occurs while getting trailer number for '
                        f'DespatchAdvice in TM flow {e}')
        return None


"""
Get mbol number for DA flow
"""
def get_mbol_number(argument_list):
    mbol = argument_list[0]
    load_doc = argument_list[1]
    try:
        for reference_number in eval('load_doc.referenceNumberStructure'):
            if eval('reference_number.referenceNumberTypeCode') == 'MB' and eval('reference_number.referenceNumber') is not None:
                return mbol
    except:
        pass
    return None



"""
Get stop reference number for DA flow
"""
def get_stop_reference_number_from_lookup(argument_list, payload = None, global_variable = None):
    return argument_list[2].get(f'{argument_list[0]}_{argument_list[1]}', None)



"""
Get despatch advice load with transcation reference type code 
as SRN
"""
def get_despatch_advice_with_srn(argument_list, payload, global_variable):
    tm_despatch = []
    tm_load = []
    path = ["despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]
    def get_data(payload, itr, path, n):
        if itr == n:
            if eval('payload.transactionalReferenceTypeCode') == "SRN" and eval('payload.entityId') is not None:
                return True
        if not isinstance(payload, list):
            try:
                items = eval('payload.' + f'{path[itr]}')
                if isinstance(items, list):
                    for index, data in enumerate(items):
                        output = get_data(data, itr + 1, path, n)
                        if output is not None:
                            return output
                else:
                    output = get_data(items, itr + 1, path, n)
                    return output
            except:
                pass
        else:
            for index, data in enumerate(payload):
                try:
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n)
                    if output is not None:
                        return output
                except:
                    pass

    for item in eval('payload.despatchAdvice'):
        if eval('item.documentActionCode') != 'DELETE':
            output = get_data(item, 0, path, 3)
            if output is not None:
                tm_despatch.append(item)
    for item in tm_despatch:
        if eval('item.despatchAdviceTransportInformation.transportLoadId') is not None:
            tm_load.append(item)

    return tm_load




"""
Get load description info based on ltl vs ftl and 
reference number type code as PRO_ID
"""
def get_load_description(argument_list):
    load_doc = argument_list[0]
    load_ref = True
    try:
        for reference_number in eval('load_doc.referenceNumberStructure'):
            if eval('reference_number.referenceNumberTypeCode') == 'PRO_ID' and eval(
                    'reference_number.referenceNumber') is not None:
                load_ref = False
                break
    except:
        pass
    if load_ref:
        try:
            load_desc = eval('argument_list[0].loadDescription')
            if not load_desc:
                if eval('argument_list[1].load.logisticServiceRequirementCode') == '2':
                    return eval('argument_list[1].load.proNumber.entityId')
                if eval('argument_list[1].load.logisticServiceRequirementCode') == '3':
                    return eval('argument_list[1].shipment.proNumber.entityId')
        except:
            return None
    return None



"""
Get trailer licenece number info for 
load update api call
"""
def get_trailer_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].trailerLicenseNumber')
        except:
            return None
    return None



"""
Get tractor license number info for 
load update api call
"""
def get_tractor_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].truckLicenseNumber')
        except:
            return None
    return None



"""
Get drive license number for load update 
api call
"""
def get_driver_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].driverLicenseNumber')
        except:
            return None
    return None



"""
Get load reference id for reference type code
WM_WH_STS_
"""
def get_load_reference_id(argument_list, payload, global_variable):
    try:
        for refrence_number_document in eval('argument_list[0].referenceNumberStructure'):
            if eval('refrence_number_document.referenceNumberTypeCode') == 'WM_WH_STS_':
                return eval('refrence_number_document.systemReferenceNumberID')
    except:
        return None



"""
Get stop status code for DA
"""
def get_stop_status(argument_list, payload, global_variable):
    return f'{argument_list[0]}{argument_list[1]}'



"""
Get stop document information for DA
"""
def get_stop_document(argument_list, payload, global_variable):
    try:
        for stop_doc in eval('argument_list[0].stop'):
            if eval('stop_doc.shippingLocationCode') == argument_list[1]:
                return stop_doc
    except:
        return None



"""
Get load status code for DA
"""
def get_load_status(argument_list, payload, global_variable):
    ans = ''
    def get_data(stop_doc):
            for ref_structure in eval('stop_doc.referenceNumberStructure'):
                if eval('ref_structure.referenceNumberTypeCode') == 'WM_SHP_STS':
                    return eval('ref_structure.referenceNumber')

    try:
        for index,stop_doc in enumerate(eval('argument_list[0].stop')):
            try:
                if eval('stop_doc.referenceNumberStructure') is not None:
                    if index == 0 and eval('stop_doc.shippingLocationCode') == argument_list[1]:
                        ans += f',{argument_list[2]}'
                    elif index == 0:
                        ans += get_data(stop_doc)
                    elif eval('stop_doc.shippingLocationCode') == argument_list[1]:
                        ans += f',{argument_list[2]}'
                    else:
                        ans += f',{get_data(stop_doc)}'
            except:
                pass
    except:
        return None
    return ans



"""
Get stop reference number for reference type code
WH_SHP_STS
"""
def get_stop_reference_number(argument_list, payload, global_variable):
    try:
        for stop_doc in eval('argument_list[0].referenceNumberStructure'):

            if eval('stop_doc.referenceNumberTypeCode') == 'WM_SHP_STS':
                    return eval('stop_doc.systemReferenceNumberID')
    except:
        return None
    return None


"""
Get tm property, fetch first element
"""
def get_tm_property(argument_list, payload, global_variable):
    return argument_list[0]


"""
Get refernce number enum and system refernce number id based on 
condition 
"""
def get_reference_number_action_enum_val(argument_list):
    referenceNumberActionEnumVal = argument_list[0]
    ref_id = argument_list[1]
    if ref_id in [None, '']:
        return {'referenceNumberActionEnumVal': 'AT_ADD'}
    else:
        return {'referenceNumberActionEnumVal': referenceNumberActionEnumVal,
                "systemReferenceNumberID": ref_id}

def get_lines_to_process(args, payload, global_variable) -> list:
    """
    Process shipments and line items based on certain conditions and update relevant information.
    Parameters:
    - args (dict): Arguments for the function.
    - payload (object): Payload object containing shipment and line item details.
    - global_variable (object): Global variables.
    Returns:
    list of lines
    """
    try:
        line_items = eval("payload.enrichment.pre_processed_da")
        shipment_details = eval('payload.enrichment.shipment')
        
        tms_wgt_uom, tms_vol_uom = args[0], args[1]

        def get_shipment_summary(shipments):
            """
            Generate a summary of shipments including the number of containers for each shipment.
            Parameters:
            - shipments (list): List of shipment objects.
            Returns:
            - dict: Shipment summary where keys are shipment numbers, and values contain the number of containers
                    and shipment details.
            """
            shipment_summary = {}
            for shipment in shipments:
                shipment_number = getattr(shipment, "shipmentNumber")
                if shipment_number in shipment_summary:
                    shipment_summary[shipment_number]["number_of_containers"] += len(getattr(shipment, "container"))
                else: 
                    shipment_summary[shipment_number] = {}
                    shipment_summary[shipment_number]["number_of_containers"] = len(getattr(shipment, "container"))
                    shipment_summary[shipment_number]["shipment_detail"] = shipment
            return shipment_summary

        def get_line_weight(line):
            wgt_value, wgt_uom, wgt_type = 0, None, None
            wgt_value = sum([getattr(getattr(trn_itm_wt, "measurementValue", {}), "value", 0) 
                for trn_item in getattr(getattr(line, "transactionalTradeItem", {}), "transactionalItemData", [])
                for trn_itm_wt in getattr(trn_item, "transactionalItemWeight", [])])
            wgt_uom = next((getattr(getattr(trn_itm_wt, "measurementValue", {}), "measurementUnitCode", 0) 
                for trn_item in getattr(getattr(line, "transactionalTradeItem", {}), "transactionalItemData", [])
                for trn_itm_wt in getattr(trn_item, "transactionalItemWeight", [])), None)
            wgt_type = next((getattr(trn_itm_wt, "measurementType", None) 
                for trn_item in getattr(getattr(line, "transactionalTradeItem", {}), "transactionalItemData", [])
                for trn_itm_wt in getattr(trn_item, "transactionalItemWeight", [])), None)
            return wgt_value, wgt_uom, wgt_type
        
        def get_line_item_summary(line_items):
            """
            Generate a summary of line items based on entity, total count, and count of items with zero despatchedQuantity.
            Parameters:
            - line_items (list): List of line items to be summarized.
            Returns:
            - dict: A dictionary containing summaries based on entity, total count, and count of items with zero despatchedQuantity.
            """
            # define the structure
            line_item_summary = defaultdict(lambda: {
                "entity": None, # Primary Key to Group lines
                "total": 0, # Count of total line(s) for a entity
                "zero": 0, # Count of line with zero quantity
                "agg_quantity": 0, # Total shipment quantity
                "agg_weight":0,  # Total shipment weight
                "agg_weight_uom": None, # Weight, source unit of measurement
                "agg_weight_type": None, # Weight, source unit of measurement
                "agg_volume": 0, # Total shipment volume
                "agg_volume_uom": None, # Volume, source unit of measurement
                "agg_volume_type": None, # Weight, source unit of measurement
            })

            for line in line_items:
                try:
                    customer_ref_entity_id = line.customerReference.entityId
                    customer_ref_line_number = line.customerReference.lineItemNumber
                    entity_id = f"{customer_ref_line_number}__{customer_ref_entity_id}"
                    # It is assumed that for lines having common entity_id will always have same:
                    # 1. input_volume_type
                    # 2. unit_volume_type
                    # 3. Weight UOM
                    # 4. Weight measurement Type
                    # 5. volume UOM
                    input_volume_type = getattr(getattr(line, "avpList", {}), "name", None)
                    unit_volume_uom = getattr(getattr(line, "avpList", {}), "qualifierCodeName", None)

                    if entity_id is None:
                        continue

                    quantity =  getattr(getattr(line, "despatchedQuantity", {}), "value", 0)
                    weight, wgt_uom, wgt_type =  get_line_weight(line)
                    gross_volume =  getattr(getattr(line, "totalGrossVolume", {}), "value", None)
                    net_volume = getattr(getattr(line, "avpList", {}), "value", 0)
                    gross_volume_uom = getattr(getattr(line, "totalGrossVolume", {}), "measurementUnitCode", None)
                    
                    if line_item_summary[entity_id]["entity"] is None:
                        # Create a new entry if entity_id not there
                        line_item_summary[entity_id]["entity"] = entity_id
                        # Field(s) that needs to be mapped once for line(s)
                        line_item_summary[entity_id]["agg_weight_uom"] = wgt_uom
                        line_item_summary[entity_id]["agg_weight_type"] = wgt_type
                        line_item_summary[entity_id]["agg_volume_uom"] = unit_volume_uom if input_volume_type == 'unitNetVolume' else gross_volume_uom
                        line_item_summary[entity_id]["agg_volume_type"] = 'unitNetVolume' if input_volume_type == 'unitNetVolume' else None

                    line_item_summary[entity_id]["zero"] += 1 if quantity == 0 else 0
                    line_item_summary[entity_id]["total"] += 1
                    line_item_summary[entity_id]["agg_quantity"] += quantity
                    line_item_summary[entity_id]["agg_weight"] += weight
                    line_item_summary[entity_id]["agg_volume"] += net_volume if input_volume_type == 'unitNetVolume' else gross_volume

                except Exception as e:
                    logger.warn(f"{e}")
            return dict(line_item_summary)
        
        def get_shipment_container_info_for_each_line(line, shipment) -> bool:
            "For a given line find the related container from the given shipment and add the required attriutes."
            ext_shipmentContainerDocument = False
            try:
                shipment_container = getattr(shipment, "container", [])
                shipmentMode = getattr(shipment, "shipmentEntryModeEnumVal", None)
                trn_ti_pk = line.srn_lineItemNumber # srn_lineItemNumber is added at enrichment layer.
                shipment_container_quantity = None
                shipment_container_weightByFreightClass = []
                for cont in shipment_container:
                    # Check for container referenceTypeCode
                    ref_status = any([ref_struct.referenceNumberTypeCode == 'WM_ORDLIN_' for ref_struct in getattr(cont, "referenceNumberStructure", [])])
                    if ref_status and cont.systemContainerID == trn_ti_pk:
                        ext_shipmentContainerDocument = True
                        shipment_container_quantity = getattr(cont, "quantity", None)
                        shipment_container_weightByFreightClass = getattr(cont, "weightByFreightClass", [])
                        break
                if ext_shipmentContainerDocument:
                    setattr(line, "ext_shipmentContainerQuantity", shipment_container_quantity)
                    setattr(line, "ext_shipmentContainerWeightByFreightClass", shipment_container_weightByFreightClass)
                    setattr(line, "ext_shipmentMode", shipmentMode)
                return ext_shipmentContainerDocument
            except Exception as e:
                logger.warn(f"{e}")
            finally:
                return ext_shipmentContainerDocument
        
        shipment_summary = get_shipment_summary(shipment_details)
        line_item_summary = get_line_item_summary(line_items)

        return_value = []
        return_value_helper = OperationUtils.dictionary_to_namespace({})
        for line in line_items:
            customer_ref_entity_id = line.customerReference.entityId
            customer_ref_line_number = line.customerReference.lineItemNumber
            key = f"{customer_ref_line_number}__{customer_ref_entity_id}"
            level_id = getattr(line, "levelId", None)
            line_summary = line_item_summary[key]
            # If line is already processed
            if line_summary.get("is_processed"):
                continue
            # Check for zero quantity
            agg_quantity = line_summary["agg_quantity"]
            if level_id != 9 and (line_summary["total"] != line_summary["zero"]) and agg_quantity != 0:
                setattr(line, "ext_shipmentToProcess", True)
                line_summary["is_processed"] = True
                # Get respective shipment for a line, customerReference.entityId (in line) == shipmentNumber (in shipment)
                shipment = shipment_summary.get(customer_ref_entity_id, {}).get("shipment_detail")
                if shipment:
                    # Add container info for the line, line(in WM) == container(in TM)
                    ext_shipmentContainerDocument = get_shipment_container_info_for_each_line(line, shipment)
                    setattr(line, "ext_shipmentContainerDocument", ext_shipmentContainerDocument)
                    # Add currentShipmentOperationalStatusEnumVal to line from respective shipment
                    setattr(line, "ext_shipmentOperationStatus", getattr(shipment, "currentShipmentOperationalStatusEnumVal", None))
                # Add aggregated quantity
                setattr(line, "ext_aggregatedQuantity", agg_quantity)
                # Add aggregated weight
                src_wgt_uom = line_summary["agg_weight_uom"]
                wgt_type = line_summary["agg_weight_type"]
                wgt_value = OperationUtils.getUomConversion('Weight', src_wgt_uom, tms_wgt_uom, line_summary["agg_weight"])
                total_gross_weight = 0
                # Shipment Aggregated Weight
                if wgt_type != 'TOTAL_GROSS_WEIGHT':
                    total_gross_weight = wgt_value
                else:
                    total_gross_weight = wgt_value * agg_quantity
                # For decimal precision handeling
                total_gross_weight = OperationUtils.getUomConversion('Weight', tms_wgt_uom, tms_wgt_uom, total_gross_weight)
                setattr(line, "ext_aggShipLineWeight", total_gross_weight)
                # Input Unit Weight
                total_gross_weight /= agg_quantity
                # For decimal precision handeling
                total_gross_weight = OperationUtils.getUomConversion('Weight', tms_wgt_uom, tms_wgt_uom, total_gross_weight)
                setattr(line, "ext_inputWeightValue", total_gross_weight)
                # Add aggregated volume
                src_vol_uom = line_summary["agg_volume_uom"]
                vol_type = line_summary["agg_volume_type"]
                vol_value = OperationUtils.getUomConversion('Volume', src_vol_uom, tms_vol_uom, line_summary["agg_volume"])
                total_volume_of_container = 0
                if vol_type != 'unitNetVolume':
                    total_volume_of_container = vol_value
                else:
                    total_volume_of_container = vol_value * agg_quantity
                # For decimal precision handeling
                total_volume_of_container = OperationUtils.getUomConversion('Volume', tms_vol_uom, tms_vol_uom, total_volume_of_container)
                # Shipment Aggregated Volume
                setattr(line, "ext_aggShipLineVolume", total_volume_of_container)
                # Input Unit Volume
                total_volume_of_container /= agg_quantity
                # For decimal precision handeling
                total_volume_of_container = OperationUtils.getUomConversion('Volume', tms_vol_uom, tms_vol_uom, total_volume_of_container)
                setattr(line, "ext_containerUnitVolume", total_volume_of_container)
                # Group line-items that belongs to same shipment w.r.t customerReference.entityId
                if hasattr(return_value_helper, customer_ref_entity_id):
                    customer_ref = getattr(return_value_helper, customer_ref_entity_id)
                    customer_ref = getattr(customer_ref, "lineItems")
                    customer_ref.append(line)
                else:
                    setattr(return_value_helper, f"{customer_ref_entity_id}", OperationUtils.dictionary_to_namespace({}))
                    customer_ref = getattr(return_value_helper, customer_ref_entity_id)
                    setattr(customer_ref, "lineItems", [line])
        for _, val in vars(return_value_helper).items():
            return_value.append(val)
        return return_value
    except Exception as e:
        logger.warn(f"{e}")
        return None
    
def get_refereneNumberStr_segment(*args):
    """
    Return the reference number structure if overRide is Planned.
    Parameters:
    - overRidePlanned (bool): Indicates whether override is planned.
    - isOverRidePlanned (bool): Indicates the actual status of override.
    - actualPalletCountField (str): Actual pallet count field.
    - actualPalletCountField_val (str): Value of actual pallet count field.
    - qty_default_key (str): Default key for quantity.
    - agg_qty_val (str): Aggregated quantity value.
    - actualVolumeField (str): Actual volume field.
    - actualVolumeField_val (str): Value of actual volume field.
    - vol_default_key (str): Default key for volume.
    - agg_vol_val (str): Aggregated volume value.
    - actualWeightField (str): Actual weight field.
    - actualWeightField_val (str): Value of actual weight field.
    - wgt_default_key (str): Default key for weight.
    - agg_wgt_val (str): Aggregated weight value.
    Returns:
    - dict: Reference number structure in the form of a dictionary.
    """
    overRidePlanned, isOverRidePlanned,\
    actualPalletCountField, actualPalletCountField_val, \
    qty_default_key, agg_qty_val, \
    actualVolumeField, actualVolumeField_val, \
    vol_default_key, agg_vol_val, \
    actualWeightField, actualWeightField_val, \
    wgt_default_key, agg_wgt_val, *_ = args[0]
    if overRidePlanned != isOverRidePlanned:
        return None
    response = {"referenceNumberStructure": []}
    if actualPalletCountField == actualPalletCountField_val:
        struct =  {
            "referenceNumberTypeCode": qty_default_key,
            "referenceNumber": agg_qty_val
        }
        response["referenceNumberStructure"].append(struct)
    if actualVolumeField == actualVolumeField_val:
        struct =  {
            "referenceNumberTypeCode": vol_default_key,
            "referenceNumber": agg_vol_val
        }
        response["referenceNumberStructure"].append(struct)
    if actualWeightField == actualWeightField_val:
        struct =  {
            "referenceNumberTypeCode": wgt_default_key,
            "referenceNumber": agg_wgt_val
        }
        response["referenceNumberStructure"].append(struct)
    return response
                  
                