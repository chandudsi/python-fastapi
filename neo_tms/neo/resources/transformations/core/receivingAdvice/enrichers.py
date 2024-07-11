from neo.log import Logger as logger
from neo.exceptions import DataException
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
import json

def shipping_location_type_enrichment_call(payload, enriched_message):
    shipping_location = []
    payload = json.loads(payload.value)
    for current_ra in payload['receivingAdvice']:
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


def load_type_enrichment_payload(payload, enriched_message):
    LoadId = []
    payload = json.loads(payload.value)
    for current_ra in payload['receivingAdvice']:
        LoadId.append({"value": current_ra['receivingAdviceTransportInformation']['transportLoadId']})

    if not LoadId:
        raise Exception("Load id is not present - EnrichmentFailed")

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


def get_updated_message_post_enrichment_call(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    message_val = json.loads(message.value)
    message_val.update({'enrichment': enriched_messages})


    message.value = message_val


    return None, None