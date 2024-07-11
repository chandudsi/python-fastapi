from neo.log import Logger as logger
from neo.exceptions import DataException
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
import json
import copy


"""
Use Case:
  Used this function for forming the shipment enrichment call.
Used In: Despatch Advice
"""
def get_shipment_enrichment_api_call_da(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    shipment_id = list()
    unique_shipment = {}
    path = ["despatchAdvice", "despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]

    def get_shipment_ids(payload, itr, path, last_child_dict, ans):
        if itr == last_child_dict:
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

    #filter to hit api only when level id is 4 or 9
    despatch_payload = payload.get('despatchAdvice', {})
    level_id_check = False
    for despatch_data in despatch_payload:
        for unit in despatch_data['despatchAdviceLogisticUnit']:
            if unit['levelId'] == 4 or unit['levelId'] == 9:
                level_id_check = True

    if not level_id_check:
        shipment_id = [{'value': 'None'}]

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
                    'name': ['id', 'systemContainerID', 'quantity', 'itemNumber', 'itemPackageLevelIDCode',
                             'contentLevelTypeCode', 'carrierPackageType', 'length', 'height', 'width',
                             'containerTypeCode', 'containerShippingInformation'],
                    'collection': [
                        {"name": "referenceNumberStructure"},
                        {"name":"weightByFreightClass"}
                    ]
                }
            }, {
                'name': 'chargeOverrides'
            }, {
                'name': 'throughPoint'
            }, {
                "name": "referenceNumberStructure"
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
Use Case:
  Used this function for forming the load update enrichment call.
Used In: Despatch Advice
"""
def load_type_enrichment_payload_da(payload, enriched_message):
    #LoadId = payload['CISDocument']['SystemLoadID']

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    LoadId = []
    if payload.get('despatchAdvice', None) is not None:
        for current_da in payload['despatchAdvice']:
            if current_da.get('despatchAdviceTransportInformation', '').get('transportLoadId', None) is not None:
                LoadId.append({"value":current_da['despatchAdviceTransportInformation']['transportLoadId']})

    else:
        LoadId.append({"value": payload['CISDocument']['SystemLoadID']})

    if not LoadId:
        raise DataException("Load id is not present - EnrichmentFailed")

    enrichment_payload = {"select": {
        "name": ['id',
                 'systemLoadID',
                 'loadDescription',
                 'totalVolume',
                 'totalPieces',
                 'totalSkids',
                 'currentLoadOperationalStatusEnumVal',
                 'currentLoadOperationalStatusEnumDescr',
                 'createdDateTime',
                 'updatedDateTime',
                 'equipmentTypeCode',
                 'trailerNumber',
                 'carrierCode',
                 'serviceCode',
                 'numberOfStops',
                 'commodityCode',
                 'totalNumberOfShipments',
                 'totalTotalPieces',
                 'totalTotalPallets',
                 'lengthUnitOfMeasureEnumVal',
                 'weightUnitOfMeasureEnumVal',
                 'totalScaledWeight',
                 'systemTripID',
                 'logisticsGroupCode',
                 'loadSequenceNumber',
                 'loadStartDateTime',
                 'loadEndDateTime',
                 'LoadSequenceNumber',
                 'Trip.SystemTripID',
                 'Trip.NumberOfLoads'
                 ],
        "collection": [
            {
                "name": "referenceNumberStructure"
            },
            {
                "name": "ShipmentLeg",
                "select": {
                    "name": [
                        'Shipment.SystemShipmentID',
                        'Shipment.ShipmentNumber',
                        'Shipment.ShipFromLocationCode',
                        'Shipment.CustomerCode',
                        'Shipment.ShipFromDescription',
                        'Shipment.ShipFromAppointmentRequiredFlag',
                        'Shipment.ShipToLocationTypeEnumVal',
                        'Shipment.ShipToLocationTypeEnumDescr',
                        'Shipment.ShipToLocationCode',
                        'Shipment.ShipToDescription',
                        'Shipment.PickupFromDateTime',
                        'Shipment.PickupToDateTime',
                        'Shipment.DeliveryFromDateTime',
                        'Shipment.DeliveryToDateTime',
                        'Shipment.CommodityCode',
                        'Shipment.ShipFromAddress',
                        'Shipment.ShipToAddress',
                        'Shipment.CreatedDateTime',
                        'Shipment.UpdatedDateTime',
                        'Shipment.InputCurrencyCode',
                        'Shipment.CurrentShipmentOperationalStatusEnumVal',
                        'Shipment.CurrentShipmentOperationalStatusEnumDescr',
                        'Shipment.UnitOfMeasure',
                        'Shipment.ShippingInformation',
                        'Shipment.SplitShipmentNumber',
                        'Shipment.NumberOfShipmentLegs',
                        'SystemShipmentLegID',
                        'SystemLoadID',
                        'ShipmentSequenceNumber',
                        'ShipFromLocationCode',
                        'ShipToLocationCode',
                        'Skids'
                    ],
                    "collection": [
                        {
                            "name": 'Shipment.ShipmentLeg',
                            "select": {
                                "name": [
                                    'SystemShipmentLegID',
                                    'ShipmentNumber',
                                    'ShipToAddress',
                                    'ShipFromAddress',
                                    'SystemLoadID',
                                    'ShipmentSequenceNumber',
                                    'ShipFromLocationCode',
                                    'ShipToLocationCode',
                                    'ShipToLocationName',
                                    'ShipFromLocationName',
                                    'ComputedDateTimeOfPickArrival',
                                    'ComputedDateTimeOfPickDeparture',
                                    'ComputedDateTimeOfDropArrival',
                                    'ComputedDateTimeOfDropDeparture',
                                    'TransitModeEnumVal',
                                    'Load.Vessel',
                                    'Load.TrailerNumber',
                                    'Load.TractorNumber',
                                    'Load.EquipmentTypeCode',
                                    'Load.TripEntityTypeEnumVal',
                                    'Load.SystemTripID',
                                    'Load.CreatedDateTime',
                                    'Load.DriverCode',
                                    'Load.TotalNumberOfShipments',
                                    'Load.CarrierCode',
                                    'Load.ServiceCode',
                                    'CarrierCode',
                                    'ServiceCode'
                                ],
                                "collection": [{
                                    "name": "Load.ReferenceNumberStructure"
                                }
                                ]
                            }
                        },
                        {
                            "name": 'Shipment.Container',
                            "select": {
                                "name": [
                                    'Id',
                                    'SystemContainerID',
                                    'Quantity',
                                    'ItemNumber',
                                    'ContainerShippingInformation.Volume',
                                    'ContainerShippingInformation.NominalWeight',
                                    'ContainerShippingInformation.Pieces',
                                    'ContainerShippingInformation.Skids',
                                    'TransportOrderInfo.SystemTransportOrderID',
                                    'TransportOrderInfo.OrderNumberCode',
                                    'TransportOrderInfo.OrderLineNumberCode',
                                    'TransportOrderInfo.OrderConsolidationGroupID',
                                    'TransportOrderInfo.ProductNumberCode',
                                    'TransportOrderInfo.ShippingInformation'
                                ],
                                "collection": [
                                    {
                                        "name": 'ReferenceNumberStructure'
                                    },
                                    {
                                        "name": 'WeightByFreightClass'
                                    },
                                    {
                                        "name": 'ShipmentItem',
                                        "select": {
                                            "collection": [
                                                {
                                                    "name": 'ReferenceNumberStructure'
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "name": 'Shipment.ReferenceNumberStructure',
                            "select": {
                                "name": [
                                    'SystemReferenceNumberID',
                                    'ReferenceNumberTypeCode',
                                    'ReferenceNumberTypeCodeDescr',
                                    'ReferenceNumber'
                                ]
                            }
                        }

                    ]
                }
            },
            {
                "name": 'Stop',
                "select": {
                    "name": [
                        'Id',
                        'SystemStopID',
                        'StopSequenceNumber',
                        'CountOfShipmentsPickedAtStop',
                        'CountOfShipmentsDroppedAtStop',
                        'ShippingLocationTypeEnumVal',
                        'ShippingLocationTypeEnumDescr',
                        'PickConfirmedFlag',
                        'Address',
                        'ShippingLocationCode',
                        'PrimaryShippingLocationCode',
                        'PrimaryShippingLocationTypeEnumVal',
                        'PrimaryShippingLocationTypeEnumDescr',
                        'WeekendHolidayBreakHours',
                        'WeekendHolidayBreakOrientationEnumVal',
                        'systemDockCommitmentID',
                        'LoadingInstructionExistFlag',
                        'PickedWeight',
                        'PickedVolume',
                        'PickedOrderValue',
                        'PickedDeclaredValue',
                        'PickedNominalWeight',
                        'PickedTareWeight',
                        'PickedPieces',
                        'PickedSkids',
                        'PickedLadenLength',
                        'DroppedWeight',
                        'DroppedVolume',
                        'DroppedOrderValue',
                        'DroppedDeclaredValue',
                        'DroppedNominalWeight',
                        'DroppedTareWeight',
                        'DroppedPieces',
                        'DroppedSkids',
                        'DroppedLadenLength',
                        'DistanceFromPreviousStop',
                        'TransitTimeFromPreviousStop',
                        'WaitingHours',
                        'LoadingHours',
                        'UnloadingHours',
                        'CustomerCode',
                        'CustomerName',
                        'DockScheduleStatusEnumVal',
                        'DockScheduleStatusEnumDescr',
                        'ShippingLocationName',
                        'AppointmentRequiredCounter',
                        'LastComputedArrivalDateTime',
                        'LastComputedDepartureDateTime',
                        'StopStatusEnumVal',
                        'StopStatusEnumDescr',
                        'Appointment',
                        'DeliveryArrivalDateTime',
                        'stopPlanningStatusEnumVal'
                    ],
                    "collection": [
                        {
                            "name": 'DropShipmentLegID'
                        },
                        {
                            "name": 'PickShipmentLegID'
                        },
                        {
                            "name": 'ReferenceNumberStructure'
                        }
                    ]
                }

            }
        ]
    },
        "condition": {
            "o": [
                {
                    "in": {
                        "o": [
                            {"name": "SystemLoadID"}, *LoadId
                        ]
                    }
                }
            ]
        }
    }
    return enrichment_payload



"""
Use Case:
  Used this function for forming the shipping location enrichment call.
Used In: Despatch Advice
"""
def shipping_location_type_enrichment_call_da(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    shipping_location = []
    if payload.get('despatchAdvice', None) is not None:

        for current_da in payload['despatchAdvice']:
            if current_da.get('shipFrom', '').get('primaryId', None) is not None:
                shipping_location.append(
                    {"and": {
                    "o": [
                        {
                            "eq": {
                                "o": [{"name": "ShippingLocationCode"}, {"value": current_da['shipFrom']['primaryId']}]
                            }
                        },
                        {
                            "ne": {
                                "o": [{"name": "ShippingLocationTypeEnumVal"}, {"value": "SPT_CONSIGNEE"}]
                            }
                        }

                    ]
                }}
                )
            if current_da.get('shipTo', '').get('primaryId', None) is not None:
                shipping_location.append(
                    {"and": {
                        "o": [
                            {
                                "eq": {
                                    "o": [{"name": "ShippingLocationCode"},
                                          {"value": current_da['shipTo']['primaryId']}]
                                }
                            },
                            {
                                "ne": {
                                    "o": [{"name": "ShippingLocationTypeEnumVal"}, {"value": "SPT_LA"}]
                                }
                            }

                        ]
                    }}
                )

    else:
        pass

    shipping_location_api_call = {
	"select": {
	   "name": ["ShippingLocationCode", "ShippingLocationTypeEnumVal", "WarehouseManagementSystemInstanceCode", "BusinessHours.TimeZoneOffset"]
	},
	"condition": {
            "o": [
                {
                    "or": {
                        "o": [
						 *shipping_location
						]
                    }
                }
            ]
        }
    }
    return shipping_location_api_call



"""
Use Case:
  Used this function for forming appending the actual
  message with enriched message
used In: Despatch Advice
"""
def form_despatch_advice_data(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val['enrichment'] = enriched_messages
        message.value = message_val
    else:
        # message.update(enriched_messages)
        message['enrichment'] = enriched_messages
    return None, None



"""
This function is used to validate if the required fields are missing
in DespatchAdviceFlow TMS
"""
def validate_despatch_advice_flow(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message.value, str):
        msg_value = json.loads(message.value)
    else:
        msg_value = message.value

    payload = msg_value.get('despatchAdvice',  {})

    for data in payload:
        if not data.get("despatchAdviceTransportInformation", {}).get("transportLoadId"):
            raise ValueError(f'Missing routeId for DespatchAdviceFlow TMS with header info: {message_hdr_info}')
        if not data.get("shipFrom", {}).get("primaryId"):
            raise ValueError(f'Missing shippingLocationCode for DespatchAdviceFlow TMS with header info: {message_hdr_info}')

    return (message, None)



"""
This function is used to validate if the required fields are missing
in DespatchAdviceFlow TMS
"""
def check_load_status_tm(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message.value, str):
        msg_value = json.loads(message.value)
    else:
        msg_value = message.value

    # load_response = msg_value.get('load', {}).get('responseHeader', {}).get('completedSuccessfully')
    load_response = msg_value.get('enrichment', {}).get('load')

    if load_response:
        for data in msg_value['enrichment']['load']:
            load_operation_status = data.get('currentLoadOperationalStatusEnumVal')

            if load_operation_status == 'S_OPEN':
                raise ValueError(f'Despatch advice message can not be processed as Load is OPEN '
                                 f'for {entity} with header info: {message_hdr_info}')
    else:
        raise ConnectionError(f'Api is not giving proper response for load search api'
                              f'for Despatch Advice in TM entity while validating load status in TM'
                              f'with header info: {message_hdr_info}')

    return (message, None)


"""
This function is used to validate if zero qty shipment present
in DespatchAdvice message TMS
"""
def check_zero_qty_shipment_exist(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message.value, str):
        msg_value = json.loads(message.value)
    else:
        msg_value = message.value

    payload = msg_value.get('despatchAdvice', {})
    for data in payload:
        for logicalUnit in data['despatchAdviceLogisticUnit']:
            if logicalUnit.get('lineItem'):
                for item in logicalUnit['lineItem']:

                    value = item.get('despatchedQuantity', {}).get('value')
                    if value == 0:
                        logger.info(f'Despatch Advice can not be processed since zero qty shipment present'
                                         f'for {entity} with header info: {message_hdr_info}')

    return (message, None)


"""
This function is used to validate if the required fields are missing
in DespatchAdviceFlow TMS
"""
def validate_service_and_carrier(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message.value, str):
        msg_value = json.loads(message.value)
    else:
        msg_value = message.value

    transport_service_level_mapping = adapter_cps_provider.get_properties()['codMapConfig'][
        'transportServiceLevelCode']

    da_msg = msg_value.get('despatchAdvice')
    for payload in da_msg:

        load_data = msg_value.get('enrichment', {}).get('load')

        service_from_da = payload.get("shipment", {}).get('transportServiceLevelCode')
        carrier_from_da = payload.get("carrier", {}).get("primaryId")

        if load_data:
            for data in msg_value['enrichment']['load']:
                carrier_from_tm = data.get('carrierCode')
                service_from_tm_key = data.get('serviceCode')
                service_from_tm = transport_service_level_mapping[service_from_tm_key]

            if service_from_da != service_from_tm:
                raise ValueError(f'Despatch Advice can not be processed since Service Code is different in TMS'
                                 f' {service_from_da} and Despatch Advice Message {service_from_tm} '
                                 f'for {entity} with header info: {message_hdr_info}')

            if carrier_from_da != carrier_from_tm:
                raise ValueError(f'Despatch Advice can not be processed since Carrier Code is different in TMS'
                                 f' {carrier_from_da} and Despatch Advice Message {carrier_from_tm} '
                                 f'for {entity} with header info: {message_hdr_info}')

        else:
            raise ConnectionError(f'Api is not giving proper response for load search api'
                                  f'for Despatch Advice in TM entity while validating Service and Carrier'
                                  f'with header info: {message_hdr_info}')

    return (message, None)


"""
This function is used to validate if the required fields are present
in DespatchAdvice message or not 
"""
def despatch_advice_preproces_translator(entity, cfg, message, trigger, trigger_options, enricher_options,
                                    enriched_messages=None, message_hdr_info = {}):
    msg_value = message
    if isinstance(message, AEHMessage) or isinstance(message, Message):
        msg_value = message.value
    # Get Despatch advice message
    DA_MSG = msg_value.get('despatchAdvice')
    # Pre-process response
    PRE_PROCESSED_DA = {
        "despatchAdvice": []
    }

    def is_line_have_srn_trn_ref(trn_ref:list = None) -> tuple:
        """Check if there is transactionalReference with SRN."""
        if not trn_ref:
            return False, None
        for ref in trn_ref:
            if ref.get("transactionalReferenceTypeCode", None) == 'SRN':
                return True, ref.get("lineItemNumber")
        return False, None

    def get_pallet_id(parent_pallet_id, DALUS):
        for dalu in DALUS:
            if dalu.get("levelId") == 3 and \
                parent_pallet_id == dalu.get("logisticUnitId", {}).get("primaryId"):
                return dalu["parentLogisticUnitId"]
        return None
    
    # For each DA in Despath-Advice message
    for DA in DA_MSG:
        doc_action_code = DA.get("documentActionCode")
        DALUS = DA.get("despatchAdviceLogisticUnit")
        LINE_ITEMS = []
        for dalu in DALUS:
            level_id = dalu.get("levelId")
            parent_level_id = dalu.get("parentLevelId")
            parent_pallet_id = dalu.get("parentLogisticUnitId")
            line_item = dalu.get("lineItem")
            if line_item:
                for line in line_item:
                    # Ignore the line if does not have atleast one transcation reference with type code as SRN
                    proceed, srn_line_item_number = is_line_have_srn_trn_ref(line.get("transactionalReference", []))
                    if not proceed:
                        continue
                    copied_line = copy.deepcopy(line)
                    copied_line["srn_lineItemNumber"] = srn_line_item_number
                    copied_line["documentActionCode"] = doc_action_code
                    copied_line["levelId"] = level_id
                    if parent_level_id == 3:
                        pallet_id = get_pallet_id(parent_pallet_id, DALUS)
                        if pallet_id:
                            copied_line["level2PalletId"] = pallet_id
                    else:
                        if parent_pallet_id:
                            copied_line["level2PalletId"] = parent_pallet_id
                    LINE_ITEMS.append(copied_line)
        PRE_PROCESSED_DA['despatchAdvice'].extend(LINE_ITEMS)
        
    return (PRE_PROCESSED_DA['despatchAdvice'], PRE_PROCESSED_DA['despatchAdvice'])
