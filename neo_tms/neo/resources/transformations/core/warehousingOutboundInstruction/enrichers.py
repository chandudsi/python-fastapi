from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
import json


"""
Use Case:
  Used this function for forming the load enrichment call.
Used In: WOI Split Shipment
"""
def get_load_enrichment_api_call_woi(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    loadId = []

    data = payload.get('warehousingOutboundInstruction', [])[0]

    load_data = data.get('shipment', [])[0].get('shipmentTransportationInformation', {}).\
        get('transportLoadId', None)

    loadId.append({"value": load_data})

    load_api_call = {
                "select": {
                    "name": [
                        "id",
                        "systemLoadID",
                        "logisticsGroupCode"
                    ],
                    "collection": [
                        {
                            "name": "shipmentLeg",

                            "select": {
                                "name": [
                                    "shipment.systemShipmentID",
                                    "shipment.shipmentNumber"
                                ],
                                "collection": [
                                    {
                                        "name": "shipment.container",
                                        "select": {
                                            "name": [
                                                "systemContainerID",
                                                "transportOrderInfo.systemTransportOrderID"
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                },
                "condition": {
                        "o": [
                            {
                                "eq": {
                                    "o": [
                                        {"name": "systemLoadID"},
                                        *loadId
                                    ]
                                }
                            }
                        ]
                    }
            }

    return load_api_call



"""
Use Case:
  Used this function for forming the shipment enrichment call.
Used In: WOI Split Shipment
"""
def get_shipment_enrichment_api_call_woi(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    shipment_id = list()
    unique_shipment = {}
    path = ["warehousingOutboundInstruction", "shipment", "shipmentItem", "transactionalReference"]

    def get_shipment_ids(payload, itr, path, last_child_dict, ans):
        if itr == last_child_dict:
            if payload['transactionalReferenceTypeCode'] == 'SRN':
                if unique_shipment.get(payload['entityId'], None) is not True:
                    unique_shipment[payload['entityId']] = True
                    return {"value": payload['entityId']}
        if not isinstance(payload, list):
            try:

                items = payload[path[itr]]
                if isinstance(items, list):
                    for data in items:
                        output = get_shipment_ids(data, itr + 1, path, last_child_dict, ans)
                        if output is not None:
                            ans.append(output)
                else:
                    output = get_shipment_ids(items, itr + 1, path, last_child_dict, ans)
                    if output is not None:
                        ans.append(output)
            except:
                pass
        else:
            for data in payload:
                try:
                    items = data[path[itr]]
                    output = get_shipment_ids(items, itr + 1, path, last_child_dict, ans)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    get_shipment_ids(payload, 0, path, len(path), shipment_id)

    shipment_api_call = {
        'select': {
            'name': ['id', 'systemShipmentID', 'shipmentNumber', 'shipFromLocationCode', 'customerCode', 'shipFromAppointmentRequiredFlag',
                     'shipToLocationTypeEnumVal', 'shipToLocationTypeEnumDescr', 'shipToLocationCode', 'shipToDescription',
                     'pickupFromDateTime', 'pickupToDateTime', 'deliveryFromDateTime', 'deliveryToDateTime', 'commodityCode',
                     'divisionCode', 'shipFromAddress', 'logisticsGroupCode', 'shipmentEntryModeEnumVal', 'shipmentPriority',
                     'freightTermsEnumVal', 'shipDirectFlag', 'shipmentConsolidationClassCode', 'shipFromLocationTypeEnumVal',
                     'shipFromLocationTypeEnumVal', 'shipFromAppointmentRequiredFlag', 'shipToAppointmentRequiredFlag', 'shipToAddress',
                     'createdDateTime', 'updatedDateTime', 'inputCurrencyCode', 'currentShipmentOperationalStatusEnumVal',
                     'currentShipmentOperationalStatusEnumDescr', 'unitOfMeasure', 'shippingInformation', 'systemLoadID',
                     'shipmentEntryTypeCode', 'splitShipmentNumber', 'itineraryTemplateCode', 'equipmentTypeCode', 'preferredAPCarrierCode',
                     'preferredAPServiceCode', 'tractorEquipmentTypeCode', 'ARServiceCode', 'urgentFlag', 'mergeInTransitConsolidationCode',
                     'mergeInTransitConsolidationNum', 'INCOTermsCode', 'buyerSellerEnumVal', 'INCOShippingLocationCode',
                     'INCOShippingLocationTypeEnumVal', 'deferAPRatingFlag', 'deferARRatingFlag', 'itemGroupCode',
                     'bookingMergeInTransitConsolidationCode', 'holdFlag', 'profitCenterCode', 'shipmentEntryVersionCode',
                     'systemPlanID', 'shipmentDescription', 'billToCustomerCode', 'splitMethodEnumVal'],
            'collection': [{
                'name': 'shipmentLeg',
                'select': {
                    'name': ['id', 'systemShipmentLegID', 'systemLoadID', 'shipmentSequenceNumber',
                             'shipFromLocationCode', 'shipFromLocationName', 'shipToLocationName',
                             'shipToLocationCode', 'shipmentNumber', 'computedDateTimeOfDropArrival',
                             'computedDateTimeOfPickDeparture', 'transitModeEnumVal',
                             'load.CurrentLoadOperationalStatusEnumVal', 'serviceCode'],
                    'collection': [{
                        'name': 'load.stop',
                        'select': {
                            'name': ['shippingLocationCode'],
                            'collection': [{
                                'name': 'referenceNumberStructure'
                            }]
                        }
                    }]
                }
            }, {
                'name': 'container',
                'select': {
                    'name': ['id', 'systemContainerID', 'quantity', 'itemNumber', 'itemDescription', 'itemPackageLevelIDCode',
                             'contentLevelTypeCode', 'carrierPackageType', 'length', 'height', 'width',
                             'containerTypeCode', 'containerShippingInformation', 'numberOfUnits', 'is3DLoadingRequiredFlag'],
                    'collection': [
                        {"name": "referenceNumberStructure"},
                        {"name": "weightByFreightClass"}
                    ]
                }
            }, {
                'name': 'chargeOverrides'
            }, {
                'name': 'throughPoint'
            }, {
                "name": "referenceNumberStructure"
            }, {
                "name": "note"
            }]
        },
        'condition': {
            'o': [{
                'or': {
                    'o': [{
                        'in': {
                            'o': [{
                                'name': 'shipmentNumber'
                            }, *shipment_id]
                        }
                    }]
                }
            }]
        }
    }

    return shipment_api_call


"""
This function is used to validate if the required fields are missing
in WOI message for Split Shipment
"""
def validate_woi_message(entity, cfg, message, trigger, trigger_options, enricher_options,
                                  enriched_messages=None, message_hdr_info={}):

    if isinstance(message.value, str):
        msg_value = json.loads(message.value)
    else:
        msg_value = message.value

    payload = msg_value.get('warehousingOutboundInstruction', [])[0]

    enriched_load_data = msg_value.get('enrichment', {}).get('load', [])[0]

    shipment_event_details = []
    shipment_enrich_details = []

    # preparing shipment event details
    for shipmentData in payload['shipment']:
        for itemData in shipmentData['shipmentItem']:
            for referenceData in itemData['transactionalReference']:
                if referenceData['transactionalReferenceTypeCode'] == 'SRN':
                    shipment_event_details.append({referenceData['entityId']: referenceData['lineItemNumber']})

    # preparing shipment enrich details
    for shipmentLegData in enriched_load_data['shipmentLeg']:
        containers = []
        for container in shipmentLegData['shipment']['container']:
            containers.append(container['systemContainerID'])

        shipment_enrich_details.append(
            {'shipment': shipmentLegData['shipment']['shipmentNumber'], 'container': containers})

    event = {}
    for item in shipment_event_details:
        for key, value in item.items():
            event.setdefault(key, []).append(value)

    validate_lines = []
    for enrich_detail in shipment_enrich_details:
        shipment = enrich_detail['shipment']
        containers = enrich_detail['container']
        status = bool(event.get(shipment, []) == containers)
        validate_lines.append({shipment: status})

    # set shipment id variable
    shipment_id = msg_value.get("warehousingOutboundInstruction",
                                [])[0].get("shipment", [])[0].get(
        'shipmentId', {}).get('primaryId', None)

    # Extract the list of shipmentItem from the payload
    shipment_items = msg_value.get("warehousingOutboundInstruction",
                                   [])[0].get("shipment", [])[0].get("shipmentItem", [])

    # Flatten the transactionalReference array and map each item to its entityId
    order_reference_ids = [ref.get("entityId", None) for item in shipment_items for ref in
                           item.get("transactionalReference", [])]

    # validation for shipment id
    if shipment_id is None:
        raise ValueError(f'Missing shipmentID for WOI Split Shipment '
                         f'with header info: {message_hdr_info}')

    # validation for orderReference id
    for eid in order_reference_ids:
        if eid is None:
            raise ValueError(f'Missing orderReferenceID for WOI Split Shipment '
                             f'with header info: {message_hdr_info}')

    return message



"""
Use Case:
  Used this function for forming appending the actual
  message with enriched message
used In: WOI Split Shipment
"""
def form_woi_data(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val['enrichment'] = enriched_messages
        message.value = message_val

        validate_woi_message(entity, cfg, message, trigger, trigger_options, enricher_options,
                                  enriched_messages, message_hdr_info)

    else:
        message['enrichment'] = enriched_messages
        validate_woi_message(entity, cfg, message, trigger, trigger_options, enricher_options,
                                           enriched_messages, message_hdr_info)

    return None, None
