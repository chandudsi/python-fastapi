from neo.exceptions import DataException
import neo.connectors.aeh as AEH
import neo.connectors.kafka as Kafka

def load_type_enrichment_payload_tl(*args):

    if isinstance(args[0], AEH.AEHMessage) or isinstance(args[0], Kafka.Message):
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


# Use Case:
#   Used this function for forming the load update enrichment call.
# Used In: TransportLoad, Despatch Advice
def load_type_enrichment_payload_shipment_leg(payload, enriched_message):
    # LoadId = payload['CISDocument']['SystemLoadID']
    if isinstance(payload, AEH.AEHMessage) or isinstance(payload, Kafka.Message):
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
        LoadId.append({"value": load_id})

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


def get_load_type_enrichment_payload(payload, enriched_message):
    if isinstance(payload, AEH.AEHMessage) or isinstance(payload, Kafka.Message):
        import json
        payload_message = json.loads(payload.value)
    else:
        payload_message = payload

    if (payload_message.get('CISDocument', {})).get('EventName', None) in ['ShipmentLegRemovedFromLoad',
                                                                     'ShipmentLegAddedToPlannedLoad']:
        return load_type_enrichment_payload_shipment_leg(payload, enriched_message)
    elif (payload_message.get('CISDocument', {})).get('EventName', None) in ['LoadTenderAccepted', 'LoadTenderUpdated',
                                                                             'LoadTenderCancelled',
                                                                             'LoadStopAppointment',
                                                                             'LoadRoutedRatedScheduled']:
        return load_type_enrichment_payload_tl(payload, enriched_message)
    return None



def form_transport_load_enriched_data(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_header_info=None):
    if isinstance(message, AEH.AEHMessage) or isinstance(message, Kafka.Message):
        import json
        msg_value = json.loads(message.value)
        payload = msg_value
        payload['enrichment'] = enriched_messages
        message.value = payload
    else:
        message['enrichment'] = enriched_messages

    return None, None


def master_service_type(*args):
    if isinstance(args[0], AEH.AEHMessage) or isinstance(args[0], Kafka.Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
    else:
        message = args[0]
        enriched_messages = args[0]
        payload = message

    serviceCode = None

    if (payload.get('CISDocument', {})).get('EventName', None) in ['ShipmentLegRemovedFromLoad',
                                                                           'ShipmentLegAddedToPlannedLoad']:
        serviceCode = payload['CISDocument'].get('LoadLeg', {}).get('SystemLoadLegService', None)

    elif (payload.get('CISDocument', {})).get('EventName', None) in ['LoadTenderAccepted', 'LoadTenderUpdated',
                                                                     'LoadTenderCancelled', 'LoadStopAppointment',
                                                                     'LoadRoutedRatedScheduled']:
        serviceCode = payload['CISDocument'].get('Service', {}).get('ServiceCode', None)
    
    if serviceCode is None:
        return None

    enrichment_payload = [
        {
            "serviceCode": serviceCode
        }
    ]

    return enrichment_payload

def dock_type_enrichment_payload_tl(*args):
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


def dock_type_enrichment_payload_for_shipment(*args):

    enrichment_payload = None
    if isinstance(args[0], AEH.AEHMessage) or isinstance(args[0], Kafka.Message):
        import json
        msg_value = json.loads(args[0].value)
        payload = msg_value
        enriched_messages = args[1]
    else:
        message = args[0]
        enriched_messages = args[1]
        payload = message
    dock_commitment_condition = {}
    loads = enriched_messages.get('loadType', None)
    if loads is None:
        return None
    if isinstance(loads, list):
        loads = loads[0]
    systemLoadId = loads.get('systemLoadID')
    load_stops = loads.get('stop')
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
                            "name": "systemLoadID"
                        },
                        {
                            "value": systemLoadId
                        }
                    ]
                }
            }
            ]
        }
    else:
        return None

    li_ = []
    for load_stop in load_stops_list:
        if load_stop is None:
            break
        if load_stop.get('systemDockCommitmentID') is not None:
            dockCommitmentId = load_stop.get('systemDockCommitmentID')
            li_.append(
                {
                    'or': {
                        'o': [{
                            'exists': {
                                'name': 'dockCommitment',
                                'condition': {
                                    'o': [{
                                        'eq': {
                                            'o': [{
                                                'name': 'systemDockCommitmentID'
                                            },
                                                {
                                                'value': dockCommitmentId
                                            }]
                                        }
                                    }]
                                }
                            }
                        }]
                    }
                }
            )

    if len(li_)>0:
        dockType_condition = {"o": [{
            "or": {
                "o": li_
            }
        }
        ]
        }

    else:
        return None

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


def get_dock_type_enrichment_payload(payload, enriched_message):
    if isinstance(payload, AEH.AEHMessage) or isinstance(payload, Kafka.Message):
        import json
        payload_message = json.loads(payload.value)
    else:
        payload_message = payload

    if (payload_message.get('CISDocument', {})).get('EventName', None) in ['ShipmentLegRemovedFromLoad',
                                                                     'ShipmentLegAddedToPlannedLoad']:
        return dock_type_enrichment_payload_for_shipment(payload, enriched_message)
    elif (payload_message.get('CISDocument', {})).get('EventName', None) in ['LoadTenderAccepted', 'LoadTenderUpdated',
                                                                             'LoadTenderCancelled',
                                                                             'LoadStopAppointment',
                                                                             'LoadRoutedRatedScheduled']:
        return dock_type_enrichment_payload_tl(payload, enriched_message)
    return None
