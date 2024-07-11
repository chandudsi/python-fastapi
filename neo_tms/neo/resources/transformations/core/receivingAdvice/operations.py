def get_unique_load_receiving_advice(argument_list, payload, global_variable):
    unique_load_id = {}
    ra_document = []
    for item in eval('payload.receivingAdvice'):
        if eval('item.documentActionCode') != 'DELETE' and eval(
                'item.receivingAdviceTransportInformation.transportLoadId', None) is not None \
                and eval('item.receivingAdviceTransportInformation.transportLoadId') != "" \
                and unique_load_id.get(eval('item.receivingAdviceTransportInformation.transportLoadId'), None) is None:
            ra_document.append(item)
    return ra_document if len(ra_document) else None

"""
    Gets the list of all line items for iteration
"""

def get_all_line_items(argument_list, payload, global_variable):
    unique_shipments = []
    items = []
    for ra in eval('payload.receivingAdvice'):
        if eval('ra.documentActionCode') != 'DELETE' and eval(
                'ra.receivingAdviceTransportInformation.transportLoadId', None) is not None \
                and eval('ra.receivingAdviceTransportInformation.transportLoadId') != "" :
            for logisticUnit in eval('ra.receivingAdviceLogisticUnit'):
                for item in eval('logisticUnit.lineItem'):
                    shipment_id = getattr(item,"transportationShipmentId",None)
                    if shipment_id not in unique_shipments and shipment_id is not None:
                        items.append(item)
                        unique_shipments.append(shipment_id)
    return items if len(items) else None

def get_enriched_load_ra(argument_list, payload, global_variable):
    try:
        op = eval('payload.enrichment.load.data')
        return op if op is not None else None
    except:
        return []

def get_unique_load_item_receiving_advice(current_ra, payload, global_variable):
    unique_load_id = {}
    ra_document = []
    if eval('current_ra[0].receivingAdviceLogisticUnit') is not None:
        if eval('current_ra[0].receivingAdviceLogisticUnit[0].lineItem') is not None:
            for lineitem in eval('current_ra[0].receivingAdviceLogisticUnit[0].lineItem'):
                if hasattr(lineitem, 'transportationShipmentId'):
                    ra_document.append(lineitem)
                    #return ra_document if len(ra_document) else None
    return ra_document if len(ra_document) else None

def get_shipment_leg_document_ra(argument_list, payload, global_variable):
    for shipment_leg in argument_list[0].shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == argument_list[1]:
            return shipment_leg
    return None

"""
    Gets shipment leg ID using Shipment Number
"""
def get_shipment_leg_id_ra(*args):
    for shipment_leg in args[0][0].shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == args[0][1]:
            return getattr(shipment_leg,"systemShipmentLegID",None)
    return None


"""
    Gets address values from shipment leg using Shipment Number
"""
def get_shipment_address_values_ra(*args):
    for shipment_leg in args[0][0].shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == args[0][1]:
            address = eval('shipment_leg.shipment.shipToAddress')
            return getattr(address,args[0][2],None)
    return None


"""
    Calculates Shipment Quantity in RA using enrichment from shipment leg
"""
def get_shipment_total_quantity_ra(*args):
    current_ra = args[0][0]
    shipmentNumber = args[0][1]
    load_document = args[0][2]
    weight_uom = args[0][3]
    shipment_leg_doc = []
    
    for shipment_leg in load_document.shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == shipmentNumber:
            shipment_leg_doc = shipment_leg

    totalQuantityReceived = get_total_quantity_received_ra([current_ra, shipmentNumber,weight_uom],None,None)
    nonAllocQuantity = get_total_non_alloc_quantity_ra([shipment_leg_doc])

    if totalQuantityReceived is not None and nonAllocQuantity is not None:
        return float(totalQuantityReceived) + float(nonAllocQuantity)

    return None

"""
    Calculates Shipment Weight in RA using enrichment from shipment leg
"""

def get_shipment_total_weight_ra(*args):
    current_ra = args[0][0]
    shipmentNumber = args[0][1]
    load_document = args[0][2]
    weight_uom = args[0][3]

    shipment_leg_doc = []
    for shipment_leg in load_document.shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == shipmentNumber:
            shipment_leg_doc = shipment_leg

    totalWeightReceived =  get_total_shipment_weight_ra([current_ra, shipmentNumber,weight_uom],None,None)
    nonAllocWeight = get_total_non_alloc_weight_ra([shipment_leg_doc])


    if totalWeightReceived is not None and nonAllocWeight is not None:
        return float(totalWeightReceived) + float(nonAllocWeight)

    return None

"""
    Gets enriched load document
"""
def get_current_load_document_ra(*args):
    for tm_enriched_load in args[0][0]:
        tm_enriched_load_id = int(eval('tm_enriched_load.systemLoadID'))
        if int(args[0][1]) == tm_enriched_load_id:
            return tm_enriched_load
    return None

"""
    Calculates total non alloc quantity
"""
def get_total_non_alloc_quantity_ra(*args):
    if hasattr(args[0][0], 'shipment'):
        shipment_list = eval('args[0][0].shipment')
        if hasattr(eval('args[0][0].shipment'), 'referenceNumberStructure'):
            for ref_str in eval('args[0][0].shipment.referenceNumberStructure'):
                if eval('ref_str.referenceNumberTypeCode', None) is not None \
                        and eval('ref_str.referenceNumberTypeCode') == 'DCS_NON_ALLO' \
                        and eval('ref_str.referenceNumber', None) is not None \
                        and eval('ref_str.referenceNumber') is True:
                    if hasattr(eval('args[0][0].shipment'), 'container'):
                        quantity = eval('args[0][0].shipment.container[0].quantity')
                        return quantity if quantity != '' else 0
            return 0
    return None


"""
    Calculates total non alloc weight
"""
def get_total_non_alloc_weight_ra(*args):
    if hasattr(args[0][0], 'shipment'):
        shipment_list = eval('args[0][0].shipment')
        if hasattr(eval('args[0][0].shipment'), 'referenceNumberStructure'):
            for ref_str in eval('args[0][0].shipment.referenceNumberStructure'):
                if eval('ref_str.referenceNumberTypeCode', None) is not None \
                        and eval('ref_str.referenceNumberTypeCode') == 'DCS_NON_ALLO' \
                        and eval('ref_str.referenceNumber', None) is not None \
                        and eval('ref_str.referenceNumber') is True \
                        and eval('args[0][0].shipment.container[0].containerShippingInformation.nominalWeight', None) is not None:
                    quantity = eval('args[0][0].shipment.container[0].quantity') * eval(
                        'currentContainer.containerShippingInformation.nominalWeight')
                    return quantity if quantity != '' else 0
            return 0
    return None

"""
    Calculates Quantity received
"""
def get_total_quantity_received_ra(*args):
    unique_load_id = {}
    total_quantity = 0
    if eval('args[0][0].receivingAdviceLogisticUnit') is not None:
        if eval('args[0][0].receivingAdviceLogisticUnit[0].lineItem') is not None:
            for lineitem in eval('args[0][0].receivingAdviceLogisticUnit[0].lineItem'):
                if hasattr(lineitem, 'transportationShipmentId') \
                        and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                        and eval('lineitem.transportationShipmentId') == args[0][1]:
                    current_value = eval('lineitem.quantityReceived.value')
                    total_quantity += current_value if current_value is not None else 0

    return total_quantity

def get_shipping_location_document_ra(ship_to_location_code, payload, global_variable):
    try:
        shipping_locations = eval('payload.shippingLocation.data')
        for shipping_location in shipping_locations:
            if hasattr('shipping_location', 'shipTo') and eval('shipping_location.shipTo') == ship_to_location_code[0]:
                return shipping_location
    except Exception as ex:
        return None
    return None

def get_total_shipment_weight_ra(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    from neo.connect.cps import adapter_cps_provider
    look_up_dict = adapter_cps_provider.get_properties().get('uom-configuration')

    def get_total_quantity_received_ra(argument_list, payload, global_variable):
        unique_load_id = {}
        total_quantity = 0
        if eval('argument_list[0][0].receivingAdviceLogisticUnit') is not None:
            if eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem') is not None:
                for lineitem in eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem'):
                    if hasattr(lineitem, 'transportationShipmentId') \
                            and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                            and eval('lineitem.transportationShipmentId') == argument_list[1]:
                        current_value = eval('lineitem.quantityReceived.value')
                        if isinstance(current_value,str): current_value = float(current_value)
                        total_quantity += current_value if current_value is not None else 0

        return total_quantity

    def get_total_weight_received_ra(argument_list, payload, global_variable):
        unique_load_id = {}
        total_quantity = 0
        current_ra=argument_list[0]
        current_receivingAdviceLU = get_value(current_ra,"receivingAdviceLogisticUnit",None)
        if current_receivingAdviceLU is not None:
            if eval('current_receivingAdviceLU[0].lineItem') is not None:
                for lineitem in eval('current_receivingAdviceLU[0].lineItem'):
                    if hasattr(lineitem, 'transportationShipmentId') \
                            and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                            and eval('lineitem.transportationShipmentId') == argument_list[1]:
                        current_value = eval(
                            'lineitem.transactionalTradeItem.transactionalItemData[0].transactionalItemWeight[0].measurementValue.value')
                        if isinstance(current_value,str): current_value = float(current_value)
                        total_quantity += current_value if current_value is not None else 0

        return total_quantity

    current_ra=argument_list[0]
    weight_format = look_up_dict.get("weight-format")
    current_item = get_value(current_ra,"receivingAdviceLogisticUnit.lineItem",None)
    if not isinstance(current_item,list):
        current_item=[current_item]
    totalWeight = get_total_weight_received_ra(argument_list, payload, global_variable)
    target_unit = argument_list[2]
    if current_item is not None:
        source_unit=None
        for item in current_item:
            if getattr(item,"transportationShipmentId",None) == argument_list[1]:
                source_unit = eval(
                    'item.transactionalTradeItem.transactionalItemData[0].transactionalItemWeight[0].measurementValue.measurementUnitCode',
                    None)
                break

        if not source_unit and source_unit == 'UNIT_NET_WEIGHT':
            totalQuantityReceived = get_total_quantity_received_ra(argument_list, payload, global_variable)
            calculated_wight = ConversionUtils.getUomConversion("Weight", source_unit, target_unit,
                                                                totalQuantityReceived * totalWeight)

            if look_up_dict.get('weight-format') is not None:
                return round(calculated_wight, len(weight_format.split('.')[-1]))
            return round(calculated_wight, 4)
        else:
            calculated_wight = ConversionUtils.getUomConversion("Weight", source_unit, target_unit,
                                                                totalWeight)
            if look_up_dict.get('weight-format') is not None:
                return round(calculated_wight, len(weight_format.split('.')[-1]))
            return round(calculated_wight, 4)
    return None

def get_total_weight_ra(argument_list):
    if argument_list[0] is not None and argument_list[1] is not None:
        return float(argument_list[0]) + float(argument_list[1])
    return None

def get_input_shipping_loc_code_ra(system_load, payload, global_variable):
    if hasattr(system_load[0], 'systemLoadID'):
        system_load_id = getattr(system_load[0], 'systemLoadID')
    else:
        return None
    for r_a in eval('payload.receivingAdvice'):
        ra_transport_id = eval('r_a.receivingAdviceTransportInformation.transportLoadId')
        if ra_transport_id == system_load_id:
            return eval('r_a.shipTo.primaryId')
    return None

def get_load_reference_number_id_ra(load_document, payload, global_variable):
    if load_document[0] is not None and eval('load_document[0].referenceNumberStructure') is not None:
        for rns in eval('load_document[0].referenceNumberStructure'):
            if eval('rns.referenceNumberTypeCode') == "WM_WH_STS_":
                return eval('rns.systemReferenceNumberID')
    return None

def get_stop_shipping_location_code_ra(argument_list, payload, global_variable):
    loadDocument = argument_list[0]
    shippingLocationCode = argument_list[1]
    if loadDocument is not None and eval('loadDocument.stop') is not None:
        for stop in eval('loadDocument.stop'):
            if eval('stop.shippingLocationCode') == shippingLocationCode:
                return eval('stop')
    return None


def get_stop_reference_number_id_ra(arguments, payload, global_variable):
    stop = arguments[0]
    shippingLocationCode = arguments[1]
    if stop is not None:
        if getattr(stop,"shippingLocationCode",None) == shippingLocationCode:
            if getattr(stop,"referenceNumberStructure",None):
                for rns in eval('stop.referenceNumberStructure',None):
                    if eval('rns.referenceNumberTypeCode') == "WM_SHP_STS":
                        return eval('rns.systemReferenceNumberID')
    return None

def get_reference_number_action_enum_for_load_ra(args):
    stopReferenceNumberID = args[0]
    if stopReferenceNumberID in [None, '']:
        return "AT_ADD"
    return args[1]

def get_stop_status(argument_list, payload, global_variable):
    return f'{argument_list[0]}-{argument_list[1]}'

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
    Returns first element of a list if it is a list
"""

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