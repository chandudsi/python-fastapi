# orderRelease Enrichers Start
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
from neo.process.system.format_checker import inbound_format
import json
from neo.log import Logger as logger
import jmespath


def item_details_enrichment_payload(*args):
    message = args[0]
    if isinstance(args[0], AEHMessage) or isinstance(args[0], Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
    else:
        msg_value = message
    enriched_messages = args[0]
    payload = msg_value
    payload_of_lineItems = payload['orderRelease'][0].get('lineItem')
    payload_of_org = payload['orderRelease'][0].get('buyer')
    output = []
    for line_item in payload_of_lineItems:
        for line_buyer in payload_of_org:
            output.append({
                "itemNumber": line_item['transactionalTradeItem']['primaryId'],
                # lineItem.transactionalTradeItem.primaryId
                "itemGroupCode": line_buyer['organisationName']  # buyer.organisationName
            })
    return output


def form_orders_enriched_data(entity, cfg, message, message_flow, trigger, trigger_options, enricher_options,
                              enriched_messages=None, message_hdr_info={}):
    import json
    msg_value = json.loads(message.value)
    payload = msg_value
    output = {
        "event": payload,
        "enrichment": enriched_messages
    }

    return None, output


# [{"shippingLocationCode": "PC0020"}]

def location_details_enrichment_payload(*args):
    import json
    message = args[0]
    msg_value = json.loads(message.value)
    enriched_messages = args[0]
    payload = msg_value
    payload_of_shipfrom = payload['orderRelease'][0].get('orderLogisticalInformation')

    output = []
    for line_item in [payload_of_shipfrom]:
        output.append({
            "shippingLocationCode": line_item['shipFrom']['primaryId'],
            # event.orderRelease.orderLogisticalInformation.shipFrom.primaryId
        })
        output.append({
            "shippingLocationCode": line_item['shipTo']['primaryId'],
            # event.orderRelease.orderLogisticalInformation.shipTo.primaryId
        })
    return output


def form_shipments_enriched_data(entity, cfg, message, message_flow, trigger, trigger_options, enricher_options,
                                 enriched_messages=None, message_hdr_info={}):
    import json
    msg_value = json.loads(message.value)
    payload = msg_value
    output = {
        "event": payload,
        "enrichment": enriched_messages
    }

    return None, output


# TM API calls payload for Transport Load - START
from neo.exceptions import DataException


def master_service_type(*args):
    message = args[0]
    enriched_messages = args[0]
    payload = message

    service_code = payload['CISDocument']['Service'].get('ServiceCode')

    post_loadType_payload = dict()
    post_loadType_payload['id'] = service_code

    return post_loadType_payload


def dock_type_enrichment_payload(*args):
    message = args[0]
    enriched_messages = args[0]
    payload = message
    dock_commitment_condition = {}
    systemLoadId = payload['CISDocument']['SystemLoadID']
    dock_code = payload['CISDocument']['SystemLoadID']
    load_stops = payload['CISDocument']['LoadStopList']
    load_stops_list = []
    if isinstance(load_stops, list):
        load_stops_list = load_stops
    else:
        load_stops_list.append(load_stops)

    if systemLoadId is not None:
        dock_commitment_condition = {
            "o": [{
                "eq": {
                    "o": [
                        {
                            "name": "SystemLoadID"
                        },
                        {
                            "value": systemLoadId
                        }
                    ]
                }
            }
            ]
        }

    li_ = []
    for load_stop in load_stops_list:
        dockId = None
        DockShippingLocationID = None
        DockShippingLocationType = 'SPT_'
        if load_stop.get('AppointmentInfo') is not None:
            if load_stop['AppointmentInfo'].get('AppointmentReferenceNumber') is not None:
                Appointment_ref_no = load_stop['AppointmentInfo']['AppointmentReferenceNumber']
                dockId = Appointment_ref_no.split(":")[1]
        DockShippingLocationID = load_stop.get('ShippingLocationCode')
        DockShippingLocationType = load_stop.get('ShippingPointType')
        li_.append(
            {
                "and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                "name": "dockCode"
                                            },
                                            {
                                                "value": dockId
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                "name": "shippingLocationCode"
                                            },
                                            {
                                                "value": DockShippingLocationID
                                            }
                                        ]
                                    }}]}
                        },
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                "name": "shippingLocationTypeEnumVal"
                                            },
                                            {
                                                "value": "SPT_" + DockShippingLocationType
                                            }
                                        ]
                                    }
                                }]}
                        }
                    ]
                }
            }
        )

    dockType_condition = {"o": [{
        "or": {
            "o": li_
        }
    }
    ]
    }

    enrichment_payload = {"select": {
        "name": ['systemDockID', 'dockCode', 'dockTypeEnumVal', 'shippingLocationCode',
                 'shippingLocationTypeEnumVal', 'inboundOutboundEnvironmentEnumVal',
                 'businessHours.timeZoneOffset', 'dockSlotIdentifier'],
        "collection": [{
            "name": 'DockCommitment',
            "condition": dock_commitment_condition,
            "select": {
                "name": ['systemDockCommitmentID', 'systemDockID', 'commitmentStartDateTime', 'commitmentEndDateTime',
                         'commitmentEndDateTime']}
        }]
    },
        "condition": dockType_condition

    }

    return enrichment_payload


def load_type_enrichment_payload_tl(*args):
    if isinstance(args[0], AEHMessage) or isinstance(args[0], Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
    else:
        message = args[0]
        enriched_messages = args[0]
        payload = message

    LoadId = payload['CISDocument']['SystemLoadID']
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
                                    'ServiceCode',
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
                        'DeliveryArrivalDateTime'
                    ],
                    "collection": [
                        {
                            "name": 'PickShipmentLegID'
                        },
                        {
                            "name": 'DropShipmentLegID'
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
                    "eq": {
                        "o": [
                            {"name": "SystemLoadID"}, {"value": LoadId}
                        ]
                    }
                }
            ]
        }
    }
    return enrichment_payload


def form_transport_load_enriched_data(entity, cfg, message, message_flow, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_header_info=None):
    if isinstance(message, AEHMessage) or isinstance(message, Message):
        import json
        msg_value = json.loads(message.value)
        payload = msg_value
        payload['enrichment'] = enriched_messages
        message.value = payload
    else:
        message['enrichment'] = enriched_messages

    return None, None


def master_service_type_for_shipment(*args):
    if isinstance(args[0], AEHMessage) or isinstance(args[0], Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
        enriched_messages = args[1]
    else:
        message = args[0]
        enriched_messages = args[1]
        payload = message
    loads = enriched_messages.get('loadType')
    if isinstance(loads, list):
        loads = loads[0]

    service_code = loads.get('serviceCode')

    post_loadType_payload = dict()
    post_loadType_payload['id'] = service_code

    return post_loadType_payload


def dock_type_enrichment_payload_for_shipment(*args):
    if isinstance(args[0], AEHMessage) or isinstance(args[0], Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
        enriched_messages = args[1]
    else:
        message = args[0]
        enriched_messages = args[1]
        payload = message
    dock_commitment_condition = {}
    loads = enriched_messages.get('loadType')
    if isinstance(loads, list):
        loads = loads[0]
    systemLoadId = loads.get('SystemLoadID')
    load_stops = loads.get('stop')
    dock_code = loads.get('SystemLoadID')
    load_stops_list = []
    if isinstance(load_stops, list):
        load_stops_list = load_stops
    else:
        load_stops_list.append(load_stops)

    if systemLoadId is not None:
        dock_commitment_condition = {
            "o": [{
                "eq": {
                    "o": [
                        {
                            "name": "SystemLoadID"
                        },
                        {
                            "value": systemLoadId
                        }
                    ]
                }
            }
            ]
        }

    li_ = []
    for load_stop in load_stops_list:
        if load_stop.get('systemDockCommitmentID') is not None:
            dockCommitmentId = load_stop.get('systemDockCommitmentID')
            li_.append(
                {
                    'or': {
                        'o': {
                            'exists': {
                                'name': 'dockCommitment',
                                'condition': {
                                    'o': {
                                        'eq': {
                                            'o': {
                                                'name': 'dockCommitmentId'
                                            },
                                            'o': {
                                                'value': dockCommitmentId
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )

    dockType_condition = {"o": [{
        "or": {
            "o": li_
        }
    }
    ]
    }

    enrichment_payload = {"select": {
        "name": ['systemDockID', 'dockCode', 'dockTypeEnumVal', 'shippingLocationCode',
                 'shippingLocationTypeEnumVal', 'inboundOutboundEnvironmentEnumVal',
                 'businessHours.timeZoneOffset', 'dockSlotIdentifier', 'dockCommitmentId'],
        "collection": [{
            "name": 'DockCommitment',
            "condition": dock_commitment_condition,
            "select": {
                "name": ['systemDockCommitmentID', 'systemDockID', 'commitmentStartDateTime', 'commitmentEndDateTime',
                         'commitmentEndDateTime']}
        }]
    },
        "condition": dockType_condition

    }

    return enrichment_payload


# TM API calls payload for Transport Load - END

def cust_enricher(*args):
    return {"customRespone": True}


# Use Case:
#   Used this function for forming the load update enrichment call.
# Used In: TransportLoad, Despatch Advice
def load_type_enrichment_payload(payload, enriched_message):
    # LoadId = payload['CISDocument']['SystemLoadID']
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        import json
        msg_value = json.loads(payload.value)
        payload = msg_value

    LoadId = []
    if payload.get('despatchAdvice', None) is not None:
        for current_da in payload['despatchAdvice']:
            if current_da.get('despatchAdviceTransportInformation', '').get('transportLoadId', None) is not None:
                LoadId.append({"value": current_da['despatchAdviceTransportInformation']['transportLoadId']})

    elif payload.get('receivingAdvice', None) is not None:
        for current_ra in payload['receivingAdvice']:
            def is_validated(current_ra):
                # route id check
                if current_ra.get('receivingAdviceTransportInformation', {}).get('transportLoadId',
                                                                                 None) is None:
                    return False
                # arrival date time
                if current_ra.get('creationDateTime', None) is None:
                    return False
                # departure date time
                if current_ra.get('receivingDateTime', '') is None:
                    return False
                # shipping location code
                if current_ra.get('shipTo', '').get('primaryId', None) is None:
                    return False
                return True

            if is_validated(current_ra):
                LoadId.append({"value": current_ra['receivingAdviceTransportInformation']['transportLoadId']})

    elif (payload.get('CISDocument', {})).get('EventName', None) in ['ShipmentLegRemovedFromLoad',
                                                                     'ShipmentLegAddedToPlannedLoad']:
        load_id = payload['CISDocument']['LoadLeg']['SystemLoadLegID']
        LoadId.append({"value": payload['CISDocument']['SystemLoadID']})

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


def get_entity_specific_shipping_location(payload):
    shipping_location = []
    if payload.get('despatchAdvice', None) is not None:
        for current_da in payload['despatchAdvice']:
            if current_da.get('shipFrom', '').get('primaryId', None) is not None:
                shipping_location.append(
                    {"and": {
                        "o": [
                            {
                                "eq": {
                                    "o": [{"name": "ShippingLocationCode"},
                                          {"value": current_da['shipFrom']['primaryId']}]
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

    elif payload.get('receivingAdvice', None) is not None:
        for current_ra in payload['receivingAdvice']:
            def is_validated(current_ra):
                # route id check
                if current_ra.get('receivingAdviceTransportInformation', '').get('transportLoadId', None) is None:
                    return False
                # arrival date time
                if current_ra.get('creationDateTime', None) is None:
                    return False
                # departure date time
                if current_ra.get('receivingDateTime', '') is None:
                    return False
                # shipping location code
                if current_ra.get('shipTo', '').get('primaryId', None) is None:
                    return False
                return True

            if is_validated(current_ra):
                if current_ra.get('shipTo', {}).get('primaryId', None) is not None and current_ra.get(
                        'documentActionCode',
                        None) not in [None,
                                      'DELETE']:
                    loc_type_value = ''
                    if current_ra.get('AdditionalFilters', {}).get('ShippingLocationTypeEnumVal', None) is not None:
                        loc_type = current_ra['AdditionalFilters']['ShippingLocationTypeEnumVal']
                        if loc_type == 'SHIP_FROM':
                            loc_type_value = 'SPT_CONSIGNEE'
                        elif loc_type == 'SHIP_TO':
                            loc_type_value = 'SPT_LA'
                        elif loc_type != '' and loc_type is not None:
                            loc_type_value = loc_type

                    shipping_location_line_item = {"and": {"o": []}}
                    shipping_location_eq = {
                        "eq": {
                            "o": [{"name": "ShippingLocationCode"},
                                  {"value": current_ra['shipTo']['primaryId']}]
                        }
                    }
                    shipping_location_ne = {
                        "ne": {
                            "o": [{"name": "ShippingLocationTypeEnumVal"}, {"value": loc_type_value}]
                        }
                    }
                    shipping_location_line_item['and']['o'].append(shipping_location_eq)
                    if loc_type_value != '' and not None:
                        shipping_location_line_item['and']['o'].append(shipping_location_ne)
                    shipping_location.append(shipping_location_line_item)
            else:
                raise DataException("Receiving Advice validation failed for shipping location enrichment")

    elif payload.get('orderRelease') is not None:
        for order in payload.get("orderRelease", []):
            if order.get("documentActionCode") not in ["DELETE", "CANCEL"]:
                ship_from_location = order.get("orderLogisticalInformation", {}).get('shipFrom', {}).get('primaryId')
                ship_to_location = order.get("orderLogisticalInformation", {}).get('shipTo', {}).get('primaryId')
                shipping_location.append({
                "and": {
                    "o": [
                        {
                            "in": {
                                "o": [{"name": "ShippingLocationCode"}, {"value": ship_from_location},
                                      {"value": ship_to_location}]
                            }
                        }
                    ]
                }
            })

    else:
        pass
    return shipping_location


# Use Case:
#   Used this function for forming the shipping location enrichment call.
# Used In: TransportLoad, Despatch Advice
def shipping_location_type_enrichment_call(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    shipping_location = get_entity_specific_shipping_location(payload)

    shipping_location_api_call = {
        "select": {
            "name": ["shippingLocationCode", "shippingLocationTypeEnumVal", "warehouseManagementSystemInstanceCode",
                     "businessHours.timeZoneOffset"],
            "collection": [{
                "name": "dock"
            }]
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
    return shipping_location_api_call if len(shipping_location) > 0 else None


# Use Case:
#   Used this function for forming the shipment enrichment call.
# Used In: TransportLoad, Despatch Advice
def get_shipment_enrichment_api_call(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        import json
        msg_value = json.loads(payload.value)
        payload = msg_value
    shipment_id = list()
    unique_shipment = {}
    path = ["despatchAdvice", "despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]

    def get_data(payload, itr, path, n, ans):
        if itr == n:
            if unique_shipment.get(payload['entityId'], None) is not True:
                unique_shipment[payload['entityId']] = True
                return {"value": payload['entityId']}
        if not isinstance(payload, list):
            try:

                items = payload[path[itr]]
                if isinstance(items, list):
                    for data in items:
                        output = get_data(data, itr + 1, path, n, ans)
                        if output is not None:
                            ans.append(output)
                else:
                    output = get_data(items, itr + 1, path, n, ans)
                    if output is not None:
                        ans.append(output)
            except:
                pass
        else:
            for data in payload:
                try:
                    items = data[path[itr]]
                    output = get_data(items, itr + 1, path, n, ans)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    get_data(payload, 0, path, 4, shipment_id)
    shipment_api_call = {
        'select': {
            'name': ['Id', 'SystemShipmentID', 'ShipmentNumber', 'ShipFromLocationCode', 'CustomerCode',
                     'ShipFromAppointmentRequiredFlag', 'ShipToLocationTypeEnumVal', 'ShipToLocationTypeEnumDescr',
                     'ShipToLocationCode', 'ShipToDescription', 'PickupFromDateTime', 'PickupToDateTime',
                     'DeliveryFromDateTime', 'DeliveryToDateTime', 'CommodityCode', 'DivisionCode', 'ShipFromAddress',
                     'LogisticsGroupCode', 'ShipmentEntryModeEnumVal', 'ShipmentPriority', 'FreightTermsEnumVal',
                     'ShipDirectFlag', 'ShipmentConsolidationClassCode', 'ShipFromLocationTypeEnumVal',
                     'ShipFromLocationTypeEnumVal', 'ShipFromAppointmentRequiredFlag', 'ShipToAppointmentRequiredFlag',
                     'ShipToAddress', 'CreatedDateTime', 'UpdatedDateTime', 'InputCurrencyCode',
                     'CurrentShipmentOperationalStatusEnumVal', 'CurrentShipmentOperationalStatusEnumDescr',
                     'UnitOfMeasure', 'ShippingInformation', 'SystemLoadID', 'ShipmentEntryTypeCode',
                     'SplitShipmentNumber', 'ItineraryTemplateCode', 'EquipmentTypeCode', 'PreferredAPCarrierCode',
                     'PreferredAPServiceCode', 'TractorEquipmentTypeCode', 'ARServiceCode', 'UrgentFlag',
                     'MergeInTransitConsolidationCode', 'MergeInTransitConsolidationNum', 'INCOTermsCode',
                     'BuyerSellerEnumVal', 'INCOShippingLocationCode', 'INCOShippingLocationTypeEnumVal',
                     'DeferAPRatingFlag', 'DeferARRatingFlag', 'ItemGroupCode',
                     'BookingMergeInTransitConsolidationCode', 'HoldFlag', 'ProfitCenterCode',
                     'ShipmentEntryVersionCode', 'SystemPlanID', 'ShipmentDescription', 'BillToCustomerCode'],
            'collection': [{
                'name': 'ShipmentLeg',
                'select': {
                    'name': ['Id', 'SystemShipmentLegID', 'SystemLoadID', 'ShipmentSequenceNumber',
                             'ShipFromLocationCode', 'ShipFromLocationName', 'ShipToLocationName', 'ShipToLocationCode',
                             'ShipmentNumber', 'ComputedDateTimeOfDropArrival', 'ComputedDateTimeOfPickDeparture',
                             'TransitModeEnumVal', 'Load.CurrentLoadOperationalStatusEnumVal', 'ServiceCode'],
                    'collection': [{
                        'name': 'Load.Stop',
                        'select': {
                            'name': ['ShippingLocationCode'],
                            'collection': [{
                                'name': 'ReferenceNumberStructure'
                            }]
                        }
                    }]
                }
            }, {
                'name': 'container',
                'select': {
                    'name': ['Id', 'SystemContainerID', 'Quantity', 'ItemNumber', 'ItemPackageLevelIDCode',
                             'ContentLevelTypeCode', 'CarrierPackageType', 'Length', 'Height', 'Width',
                             'ContainerTypeCode', 'ContainerShippingInformation']
                }
            }, {
                'name': 'ChargeOverrides'
            }, {
                'name': 'ThroughPoint'
            }]
        },
        'condition': {
            'o': [{
                'or': {
                    'o': [{
                        'in': {
                            'o': [{
                                'name': 'ShipmentNumber'
                            }, *shipment_id]
                        }
                    }]
                }
            }]
        }
    }
    return shipment_api_call


# Use Case:
#   Used this function for forming appending the actual
#   message with enriched message
# used In: Despatch Advice
def form_despatch_advice_data(entity, cfg, message, trigger, trigger_options, enricher_options,
                              enriched_messages=None, message_hdr_info={}):
    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val.update(enriched_messages)
        message.value = message_val
    else:
        message.update(enriched_messages)
    return None, None


def get_updated_message_post_enrichment_call(entity, cfg, message, trigger, trigger_options, enricher_options,
                                             enriched_messages=None, message_hdr_info={}):
    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val.update({'enrichment': enriched_messages})
        message.value = message_val
    else:
        message.update({'enrichment': enriched_messages})

    return None, None


def load_type_enrichment_payload_for_WON(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        import json
        msg_value = json.loads(payload.value)
        payload = msg_value
    load_id_list = set(jmespath.search(
        'warehousingOutboundNotification[].shipment[].shipmentTransportationInformation.transportLoadId', payload))
    load_ids = [{"value": id} for id in load_id_list]
    return {"select": {
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
                        {'name': 'referenceNumberStructure'},
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
                        'stopPlanningStatusEnumVal',
                        'ShippingLocationCode',
                        'PrimaryShippingLocationCode',
                        'PrimaryShippingLocationTypeEnumVal',
                        'PrimaryShippingLocationTypeEnumDescr',
                        'WeekendHolidayBreakHours',
                        'WeekendHolidayBreakOrientationEnumVal',
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
                        'DeliveryArrivalDateTime'
                    ],
                    "collection": [
                        {
                            "name": 'PickShipmentLegID'
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
                            {"name": "SystemLoadID"}, *load_ids
                        ]
                    }
                }
            ]
        }
    }


def get_shipment_enrichment_api_call_for_WON(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        import json
        msg_value = json.loads(payload.value)
        payload = msg_value
    # expression= 'warehousingOutboundNotification[].shipment[].shipmentItem[].transactionalReference[].entityId'
    expression = 'warehousingOutboundNotification[].shipment[].transactionalReference[].entityId'

    def _get_shipment_enrichment_api_call(payload, enriched_message, expression):
        shipment_id_list = set(jmespath.search(expression, payload))
        shipment_id = [{"value": i} for i in shipment_id_list]

        shipment_api_call = {
            'select': {
                'name': ['Id', 'SystemShipmentID', 'ShipmentNumber', 'ShipFromLocationCode', 'CustomerCode',
                         'ShipFromAppointmentRequiredFlag', 'ShipToLocationTypeEnumVal', 'ShipToLocationTypeEnumDescr',
                         'ShipToLocationCode', 'ShipToDescription', 'PickupFromDateTime', 'PickupToDateTime',
                         'DeliveryFromDateTime', 'DeliveryToDateTime', 'CommodityCode', 'DivisionCode',
                         'ShipFromAddress',
                         'LogisticsGroupCode', 'ShipmentEntryModeEnumVal', 'ShipmentPriority', 'FreightTermsEnumVal',
                         'ShipDirectFlag', 'ShipmentConsolidationClassCode', 'ShipFromLocationTypeEnumVal',
                         'ShipFromLocationTypeEnumVal', 'ShipFromAppointmentRequiredFlag',
                         'ShipToAppointmentRequiredFlag',
                         'ShipToAddress', 'CreatedDateTime', 'UpdatedDateTime', 'InputCurrencyCode',
                         'CurrentShipmentOperationalStatusEnumVal', 'CurrentShipmentOperationalStatusEnumDescr',
                         'UnitOfMeasure', 'ShippingInformation', 'SystemLoadID', 'ShipmentEntryTypeCode',
                         'SplitShipmentNumber', 'ItineraryTemplateCode', 'EquipmentTypeCode', 'PreferredAPCarrierCode',
                         'PreferredAPServiceCode', 'TractorEquipmentTypeCode', 'ARServiceCode', 'UrgentFlag',
                         'MergeInTransitConsolidationCode', 'MergeInTransitConsolidationNum', 'INCOTermsCode',
                         'BuyerSellerEnumVal', 'INCOShippingLocationCode', 'INCOShippingLocationTypeEnumVal',
                         'DeferAPRatingFlag', 'DeferARRatingFlag', 'ItemGroupCode',
                         'BookingMergeInTransitConsolidationCode', 'HoldFlag', 'ProfitCenterCode',
                         'ShipmentEntryVersionCode', 'SystemPlanID', 'ShipmentDescription', 'BillToCustomerCode'],
                'collection': [{
                    'name': 'ShipmentLeg',
                    'select': {
                        'name': ['Id', 'SystemShipmentLegID', 'SystemLoadID', 'ShipmentSequenceNumber',
                                 'ShipFromLocationCode', 'ShipFromLocationName', 'ShipToLocationName',
                                 'ShipToLocationCode',
                                 'ShipmentNumber', 'ComputedDateTimeOfDropArrival', 'ComputedDateTimeOfPickDeparture',
                                 'TransitModeEnumVal', 'Load.CurrentLoadOperationalStatusEnumVal', 'ServiceCode'],
                        'collection': [{
                            'name': 'Load.Stop',
                            'select': {
                                'name': ['ShippingLocationCode'],
                                'collection': [{
                                    'name': 'ReferenceNumberStructure'
                                }]
                            }
                        }]
                    }
                }, {
                    'name': 'container',
                    'select': {
                        'name': ['Id', 'SystemContainerID', 'Quantity', 'ItemNumber', 'ItemPackageLevelIDCode',
                                 'ContentLevelTypeCode', 'CarrierPackageType', 'Length', 'Height', 'Width',
                                 'ContainerTypeCode', 'ContainerShippingInformation', 'numberOfUnits'],

                        'collection': [
                            {
                                'name': 'ReferenceNumberStructure'
                            },
                            {
                                "name": 'WeightByFreightClass'
                            }
                        ]
                    }
                }, {
                    'name': 'ChargeOverrides'
                }, {
                    'name': 'ThroughPoint'
                }, {
                    'name': 'ReferenceNumberStructure'
                }]
            },
            'condition': {
                'o': [{
                    'or': {
                        'o': [{
                            'in': {
                                'o': [{
                                    'name': 'ShipmentNumber'
                                }, *shipment_id]
                            }
                        }]
                    }
                }]
            }
        }
        return shipment_api_call

    return _get_shipment_enrichment_api_call(payload, enriched_message, expression)


def dock_type_enrichment_payload_tc(*args):
    message = args[0]
    enriched_messages = args[0]
    payload = message
    doc1, dock_condition2, dock_condition3, dock_condition4 = [], [], [], []
    dock_condition5, dock_condition6, dock_condition9 = [], [], []
    dock_condition7, dock_condition8, dock_condition1 = {}, {}, {}
    confirmation_list = payload['transportPickUpDropOffConfirmation']
    for confirmation_dict in confirmation_list:
        action_code = confirmation_dict['documentActionCode']
        if action_code != "DELETE":
            SystemLoadID = None
            DockCommitmentID = confirmation_dict.get('transportPickUpDropOffConfirmationId')
            shippingLocationCode = confirmation_dict['slotProvider']['primaryId']
            DockID = None
            DockShippingLocationType = "SPT_DC"
            # DockShippingLocationType = confirmation_dict.get('slotProvider.primaryId')
            if confirmation_dict.get('transportAppointmentTypeCode') == 'PICK_UP':
                DockTypeEnumVal = 'DKT_OUTBOUND'
            else:
                DockTypeEnumVal = 'DKT_INBOUND'
            if confirmation_dict['plannedDropOff']['logisticLocation']['sublocationId'] != None:
                DockSlotIdentifier = confirmation_dict['plannedDropOff']['logisticLocation']['sublocationId']
            else:
                DockSlotIdentifier = confirmation_dict['plannedPickUp']['logisticLocation']['sublocationId']

            if SystemLoadID is not None:
                dock_condition1.append({
                    "or": {
                        "o": [{
                            "eq": {
                                "o": [
                                    {
                                        "name": "Load.SystemLoadID"
                                    },
                                    {
                                        "value": SystemLoadID
                                    }
                                ]
                            }
                        }
                        ]
                    }})
            else:
                doc1.append({
                    "value": DockCommitmentID
                }),
                dock_condition1 = {
                    "or": {
                        "o": [{
                            "in": {
                                "o": [
                                    {
                                        "name": "SystemDockCommitmentID"
                                    },
                                    *doc1
                                ]
                            }
                        }
                        ]
                    }}

            if shippingLocationCode is not None:
                dock_condition2.append(
                    {
                        "or":
                            {
                                "o": [
                                    {"eq": {
                                        "o": [
                                            {
                                                "name": 'ShippingLocationCode'
                                            },
                                            {
                                                "value": shippingLocationCode
                                            }
                                        ]
                                    }
                                    }
                                ]
                            }})

            if DockID is not None:
                dock_condition3.append({"and": {
                    "o": [
                        {"eq": {
                            "o": [
                                {
                                    "name": 'DockCode'
                                },
                                {
                                    "value": DockID
                                }
                            ]
                        }
                        }
                    ]
                }})

            if DockShippingLocationType is not None:
                dock_condition4.append({"and": {
                    "o": [
                        {"eq": {
                            "o": [
                                {
                                    "name": 'shippingLocationTypeEnumVal'
                                },
                                {
                                    "value": DockShippingLocationType
                                }
                            ]
                        }
                        }
                    ]}})
            if DockTypeEnumVal == 'DKT_OUTBOUND':
                dock_condition5.append({"and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                "name": 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_OUTBOUND'
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_BOTH'
                                            }
                                        ]
                                    }}]}
                        }]}})

            if DockTypeEnumVal == 'DKT_INBOUND':
                dock_condition6.append({"and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_INBOUND'
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_BOTH'
                                            }
                                        ]
                                    }}]}
                        }]}})
            if DockTypeEnumVal == 'DKT_OUTBOUND':
                dock_condition7 = {"and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_OUTBOUND'
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_BOTH'
                                            }
                                        ]
                                    }}]}
                        }]}}
            if DockTypeEnumVal == 'DKT_INBOUND':
                dock_condition8 = {"and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_INBOUND'
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockTypeEnumVal'
                                            },
                                            {
                                                'value': 'DKT_BOTH'
                                            }
                                        ]
                                    }}]}
                        }]}}
            if DockSlotIdentifier is not None:
                dock_condition9.append({"and": {
                    "o": [
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'DockSlotIdentifier'
                                            },
                                            {
                                                'value': DockSlotIdentifier
                                            }
                                        ]
                                    }
                                }]}},
                        {
                            "and": {
                                "o": [{
                                    "eq": {
                                        "o": [
                                            {
                                                'name': 'ShippingLocationCode'
                                            },
                                            {
                                                'value': shippingLocationCode
                                            }
                                        ]
                                    }}]}
                        }, dock_condition7,
                        dock_condition8
                    ]}})

    enrichment_payload = {"select": {
        "name": ['SystemDockID',
                 'DockCode',
                 'DockTypeEnumVal',
                 'ShippingLocationCode',
                 'ShippingLocationTypeEnumVal',
                 'InboundOutboundEnvironmentEnumVal',
                 'BusinessHours.TimeZoneOffset',
                 'DockSlotIdentifier'
                 ],
        "collection": [
            {
                "name": "DockCommitment",
                "condition":
                    {
                        "o": [
                            dock_condition1
                        ]
                    },
                "select": {
                    "name": ['SystemDockCommitmentID',
                             'SystemDockID',
                             'CommitmentStartDateTime',
                             'CommitmentEndDateTime',
                             'CarrierCode',
                             'Load.SystemLoadID',
                             'Load.CurrentLoadOperationalStatusEnumVal',
                             'Load.TrailerNumber',
                             'Load.EquipmentTypeCode'
                             ],
                    "collection": [
                        {
                            "name": "Load.Stop",
                            "condition": {
                                "o": [
                                    *dock_condition2
                                ]
                            },
                            "Select": {
                                "name": [
                                    'SystemStopID',
                                    'StopTypeEnumVal',
                                    'CountOfShipmentsPickedAtStop',
                                    'CountOfShipmentsDroppedAtStop'
                                ]}}]}}]},
        "condition": {
            "o": [{
                "or": {
                    "o": [
                        *dock_condition3,
                        {
                            "and": {
                                "o": [
                                    {"eq": {
                                        "o": [
                                            {
                                                'name': 'ShippingLocationCode'
                                            },
                                            {
                                                'value': shippingLocationCode
                                            }
                                        ]
                                    }
                                    }
                                ]}},
                        *dock_condition4,
                        *dock_condition5,
                        *dock_condition6,
                        *dock_condition9
                    ]
                }}]}}
    return enrichment_payload


def form_transport_confirmation_data(entity, cfg, message, message_flow, trigger, trigger_options, enricher_options,
                                     enriched_messages=None, message_header=None):

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        import json
        msg_value = json.loads(message.value)
        payload = msg_value
        payload['enrichment'] = enriched_messages
        message.value = payload
    else:
        message['enrichment'] = enriched_messages
    return None, None


def cust_enricher_test(*args):
    return {'key': 2}, {'value': 3}


def get_shipment_entry_type_enrichment_request_body(*args):
    """
    This method is used for creating the shipment entry type enrichment API call for order Release message
    :param: args (list of argument which is having the orderRelease message).
    """
    payload = args[0]
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    customer_code_list = []
    for order in payload.get("orderRelease", []):
        order.get("documentActionCode") not in ["DELETE", "CANCEL"]
        return {
            "select": {
                "name": [
                    "id",
                    "shipmentEntryTypeCode",
                    "shipmentEntryModeEnumVal"
                ]
            }
        }


def get_customer_type_enrichment_request_body(*args):
    """
    This method is used for creating the customer enrichment API call for order Release message
    :param: args (list of argument which is having the orderRelease message).
    """
    payload = args[0]
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    customer_code_list = []
    for order in payload.get("orderRelease", []):
        customer_code = order.get("tmCustomerCode", None)
        if order.get("documentActionCode") not in ["DELETE", "CANCEL"] and not customer_code:
            customer_code = order.get("seller", {}).get("primaryId")
        customer_code_list.append({"value": customer_code})
    api_payload = {
        "select": {
            "name": ["customerCode", "itemLevelDetailEnumVal", "defaultShipmentEntryTypeCode"]
        },
        "condition": {
            "o": [
                {
                    "in": {
                        "o": [{"name": "customerCode"}, *customer_code_list]
                    }
                }
            ]
        }
    }
    return api_payload


def get_shipment_enrichment_request_body(*args):
    """
    This method is used for creating the shipment enrichment API call for order Release message
    :param: args (list of argument which is having the orderRelease message).
    """
    payload = args[0]
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    order_list = []
    for order in payload.get("orderRelease", []):
        order_list.append({"value": order.get("orderId")})
    api_payload = {
        "select": {
            "name": [
                "Id", "SystemShipmentID", "ShipmentNumber", "ShipFromLocationCode", "CustomerCode",
                "ShipFromAppointmentRequiredFlag", "ShipToLocationTypeEnumVal", "ShipToLocationTypeEnumDescr",
                "ShipToLocationCode", "ShipToDescription", "PickupFromDateTime", "PickupToDateTime",
                "DeliveryFromDateTime", "DeliveryToDateTime", "CommodityCode", "DivisionCode", "ShipFromAddress",
                "LogisticsGroupCode", "ShipmentEntryModeEnumVal", "ShipmentPriority", "FreightTermsEnumVal",
                "ShipDirectFlag", "ShipmentConsolidationClassCode", "ShipFromLocationTypeEnumVal",
                "ShipFromLocationTypeEnumVal", "ShipFromAppointmentRequiredFlag", "ShipToAppointmentRequiredFlag",
                "ShipToAddress", "CreatedDateTime", "UpdatedDateTime", "InputCurrencyCode",
                "CurrentShipmentOperationalStatusEnumVal", "CurrentShipmentOperationalStatusEnumDescr",
                "UnitOfMeasure", "ShippingInformation", "SystemLoadID", "ShipmentEntryTypeCode",
                "SplitShipmentNumber", "ItineraryTemplateCode", "EquipmentTypeCode", "PreferredAPCarrierCode",
                "PreferredAPServiceCode", "TractorEquipmentTypeCode", "ARServiceCode", "UrgentFlag",
                "MergeInTransitConsolidationCode", "MergeInTransitConsolidationNum", "INCOTermsCode",
                "BuyerSellerEnumVal", "INCOShippingLocationCode", "INCOShippingLocationTypeEnumVal",
                "DeferAPRatingFlag", "DeferARRatingFlag", "ItemGroupCode", "BookingMergeInTransitConsolidationCode",
                "HoldFlag", "ProfitCenterCode", "ShipmentEntryVersionCode", "SystemPlanID", "ShipmentDescription",
                "BillToCustomerCode", "Memo"
            ],
            "collection": [
                {
                    "name": "ShipmentLeg",
                    "select": {
                        "name": [
                            "Id", "SystemShipmentLegID", "SystemLoadID", "ShipmentSequenceNumber",
                            "ShipFromLocationCode", "ShipFromLocationName", "ShipToLocationName",
                            "ShipToLocationCode", "ShipmentNumber", "ComputedDateTimeOfDropArrival",
                            "ComputedDateTimeOfPickDeparture", "TransitModeEnumVal",
                            "Load.CurrentLoadOperationalStatusEnumVal", "ServiceCode", "ShipToAddress",
                            "ShipFromAddress", "ComputedDateTimeOfPickArrival", "ComputedDateTimeOfPickDeparture",
                            "ComputedDateTimeOfDropArrival", "ComputedDateTimeOfDropDeparture",
                            "Load.CurrentLoadOperationalStatusEnumVal", "TransitModeEnumVal", "Load.Vessel",
                            "Load.TrailerNumber", "Load.TractorNumber", "Load.EquipmentTypeCode",
                            "Load.TripEntityTypeEnumVal", "Load.SystemTripID", "Load.CreatedDateTime",
                            "Load.DriverCode", "Load.CarrierCode", "Load.ServiceCode",
                            "Load.TotalNumberOfShipments",
                            "CarrierCode", "ServiceCode"
                        ],
                        "collection": [
                            {
                                "name": "Load.Stop",
                                "select": {
                                    "name": [
                                        "ShippingLocationCode"
                                    ],
                                    "collection": [
                                        {
                                            "name": "ReferenceNumberStructure"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "name": "ReferenceNumberStructure"
                },
                {
                    "name": "container",
                    "select": {
                        "name": [
                            "Id", "SystemContainerID", "Quantity", "ItemNumber",
                            "ItemPackageLevelIDCode", "ContentLevelTypeCode", "CarrierPackageType",
                            "Length", "Height", "Width", "ContainerTypeCode", "ContainerShippingInformation"
                        ],
                        "collection": [
                            {
                                "name": "ReferenceNumberStructure"
                            },
                            {
                                "name": "WeightByFreightClass"
                            },
                            {
                                "name": "ShipmentItem"
                            }
                        ]
                    }
                },
                {
                    "name": "ChargeOverrides"
                },
                {
                    "name": "ThroughPoint"
                }
            ]
        },
        "condition": {
            "o": [
                {
                    "exists": {
                        "name": "ShipmentLeg",
                        "condition": {
                            "o": [
                                {
                                    "eq": {
                                        "o": [
                                            {
                                                "name": "ShipmentNumber"
                                            },
                                            *order_list
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        }
    }
    return api_payload


def validate_orderRelease_inbound_message(*args):
    """
    This method is used for doing the pre-checks of the incoming message data order Release message
    :param: args (list of argument which is having the orderRelease message).
    """
    payload = args[2]
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    list_of_order = payload.get("orderRelease", [])
    do_prechecks_on_order_list_tms(list_of_order)
    modify_additional_primary_id_tms(list_of_order)
    return None, None


def modify_additional_primary_id_tms(payload_list):
    """
    This function is used for modifying the ship to primaryIa
    If it's not passed in the input feed.
    :param payload_list: order Release list
    :return:
    """
    qualifier_name = f'modify_additional_primary_id_tms'
    log_text = f'{qualifier_name}'
    look_up_dict = adapter_cps_provider.get_properties().get("codMapConfig")
    for order in payload_list:
        if not order.get("orderLogisticalInformation", {}).get("shipTo", {}).get("primaryId", {}):
            lookup = None
            # get the consigneeLookupField from the property file to prepare the primary Id for order.
            if look_up_dict.get("co.consigneeLookupField", None) == "postalcode":
                logger.info(f'{log_text}, Inside postal code lookup for orderId: {order.get("orderId")}')
                postal_code = order.get("orderLogisticalInformation").get("shipTo").get("address"). \
                    get("postalCode")
                # get the significant digit from the property file to make the loop up for postal code.
                postal_code_digit = look_up_dict.get("co.postalcode.significant.digits", [])  # Property
                lookup = look_up_dict.get(
                    "PO." + postal_code[0:int(postal_code_digit)])  # lookup in property file for this
            elif look_up_dict.get("co.consigneeLookupField", None) == "state":
                logger.info(f'{log_text}, Inside state lookup for orderId: {order.get("orderId")}')
                state = order.get("orderLogisticalInformation").get("shipTo").get("address"). \
                    get("state")
                lookup = look_up_dict.get("STATE." + state, None)  # look in the property table
            if lookup:
                order["orderLogisticalInformation"]["shipTo"]["primaryId"] = lookup


def do_prechecks_on_order_list_tms(payload_list):
    """
     This function is responsible for doing the necessary check before processing the
     actual create/update shipment
    :param payload_list: List of order message which is received from the streaming pipeline
    :return: If all required pre-check will be fine it will return None otherwise it will through the Exception with
             proper error message.
    """
    for order in payload_list:
        current_order_id = order.get("orderId")
        document_action_code = order.get("documentActionCode")

        logger.info(f" Validating orderRelease with orderId: {current_order_id}"
                    f" and documentActionCode: {document_action_code} ")
        if document_action_code == "ADD":
            required_field = {
                "buyer_identification": order.get("buyer",{}).get("primaryId"),
                "seller_identification": order.get("supplier",{}).get("primaryId"),
                "ship_to_location_code": order.get("orderLogisticalInformation",{}).get("shipTo",{}).get("primaryId"),
                "ship_from_additional_party_identification": order.get("orderLogisticalInformation",{}).get(
                    "shipFrom",{}).get("primaryId"),
                "order_priority_with_spaces": order.get("orderPriority"),
                "requested_ship_date_range_begin_date": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedShipDateRange",{}).get("beginDate"),
                "requested_ship_date_range_begin_time": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedShipDateRange",{}).get("beginTime"),
                "requested_ship_date_range_end_date": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedShipDateRange",{}).get("endDate"),
                "requested_ship_date_range_end_time": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedShipDateRange",{}).get("endTime"),
                "requested_delivery_date_range_begin_date": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedDeliveryDateRange",{}).get("beginDate"),
                "requested_delivery_date_range_begin_time": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedDeliveryDateRange",{}).get("beginTime"),
                "requested_delivery_date_range_end_date": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedDeliveryDateRange",{}).get("endDate"),
                "requested_delivery_date_range_end_time": order.get("orderLogisticalInformation",{}).get(
                    "orderLogisticalDateInformation",{}).get("requestedDeliveryDateRange",{}).get("endTime")
            }
            if required_field.get("order_priority_with_spaces"):
                required_field["order_priority_with_spaces"] = str(required_field.get("order_priority_with_spaces")
                                                                   ).strip()
            for item, value in required_field.items():
                if not required_field.get(item):
                    raise Exception(
                        f'Missing {item} with Value: {value} for orderId: {current_order_id}')
        else:
            logger.info(f"Valid orderRelease Message which is having the orderId: {current_order_id} "
                        f"and documentActionCode: {document_action_code}")
