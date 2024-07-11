import jmespath
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
import json
from neo.log import Logger

# Use Case:
#   Used this function for forming appending the actual
#   message with enriched message
# used In: WarehousingOutboundNotification


def form_won_enriched_data(entity, cfg, message, trigger, trigger_options, enricher_options,
                           enriched_messages=None, message_hdr_info=None):

    """
    Forms enriched data based on incoming messages and enrichment options.

    :param entity: The entity associated with the data enrichment.
    :param cfg: Configuration settings for the enrichment process.
    :param message: The incoming message to be enriched.
    :param trigger: The trigger that initiated the enrichment.
    :param trigger_options: Options related to the trigger.
    :param enricher_options: Options specific to the enrichment process.
    :param enriched_messages: Optional. Existing enriched messages to be updated.
    :param message_hdr_info: Optional. Additional header information for the message.

    :returns:
        tuple: A tuple containing two elements. The first element is always None,
        representing no error during the enrichment process. The second element
        is a dictionary containing the enriched data.
    """
    Logger.debug("Starting form_won_enriched_data function.", message_hdr_info)

    # Initialize message header info if not provided.
    if message_hdr_info is None:
        message_hdr_info = {}

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value

        # Create a dictionary containing the event and enriched data.
        output = {
            "event": msg_value,
            "enrichment": enriched_messages
        }

        Logger.debug("Enrichment completed successfully for incoming message.", message_hdr_info)
        return None, output # Return enriched data.

    else:
        # Update the incoming message with enriched data.
        message.update(enriched_messages)

        Logger.debug("Enriched data added to the incoming message.", message_hdr_info)

    Logger.debug("Exiting form_won_enriched_data function.", message_hdr_info)
    return None, message


def get_load_type_enrichment_api_request_body_for_WON(payload, enriched_message, message_hdr_info=None):
    """
    Creates a load type enrichment API request body for a Warehousing Outbound Notification (WON).

    :param payload: AEHMessage/KafkaMessage - The incoming message containing relevant data.
    :param enriched_message: Enriched messages - Enriched data for the payload.
    :param message_hdr_info: Optional. Additional header information for the message.

    :returns:
        dict: A dictionary representing the API request body for load type enrichment.
    """
    # Initialize message header info if not provided.
    if message_hdr_info is None:
        message_hdr_info = {}

    Logger.debug("Starting get_load_type_enrichment_api_request_body_for_WON function.", message_hdr_info)

    # Check if the payload is of type AEHMessage or KafkaMessage and extract its value.
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    # # Extract unique load IDs from the payload.
    load_id_list = set(jmespath.search(
        'warehousingOutboundNotification[].shipment[].shipmentTransportationInformation.transportLoadId', payload))

    # Convert load IDs into a list of dictionaries.
    load_number = [{"value": loads} for loads in load_id_list]

    # Define the structure of the load type enrichment API request body.
    load_api_request = {
        "select": {
            "name": [
                "id",
                "systemLoadID",
                "loadDescription",
                "totalVolume",
                "totalPieces",
                "totalSkids",
                "currentLoadOperationalStatusEnumVal",
                "currentLoadOperationalStatusEnumDescr",
                "createdDateTime",
                "updatedDateTime",
                "equipmentTypeCode",
                "trailerNumber",
                "carrierCode",
                "serviceCode",
                "numberOfStops",
                "commodityCode",
                "totalNumberOfShipments",
                "totalTotalPieces",
                "totalTotalPallets",
                "lengthUnitOfMeasureEnumVal",
                "weightUnitOfMeasureEnumVal",
                "totalScaledWeight",
                "systemTripID",
                "logisticsGroupCode",
                "loadStartDateTime",
                "loadEndDateTime",
                "LoadSequenceNumber",
                "Trip.SystemTripID",
                "Trip.NumberOfLoads"
            ],
            "collection": [
                {
                    "name": "referenceNumberStructure"
                },
                {
                    "name": "ShipmentLeg",
                    "select": {
                        "name": [
                            "Shipment.SystemShipmentID",
                            "Shipment.ShipmentNumber",
                            "Shipment.ShipFromLocationCode",
                            "Shipment.CustomerCode",
                            "Shipment.ShipFromDescription",
                            "Shipment.ShipFromAppointmentRequiredFlag",
                            "Shipment.ShipToLocationTypeEnumVal",
                            "Shipment.ShipToLocationTypeEnumDescr",
                            "Shipment.ShipToLocationCode",
                            "Shipment.ShipToDescription",
                            "Shipment.PickupFromDateTime",
                            "Shipment.PickupToDateTime",
                            "Shipment.DeliveryFromDateTime",
                            "Shipment.DeliveryToDateTime",
                            "Shipment.CommodityCode",
                            "Shipment.ShipFromAddress",
                            "Shipment.ShipToAddress",
                            "Shipment.CreatedDateTime",
                            "Shipment.UpdatedDateTime",
                            "Shipment.InputCurrencyCode",
                            "Shipment.CurrentShipmentOperationalStatusEnumVal",
                            "Shipment.CurrentShipmentOperationalStatusEnumDescr",
                            "Shipment.UnitOfMeasure",
                            "Shipment.ShippingInformation",
                            "Shipment.SplitShipmentNumber",
                            "Shipment.NumberOfShipmentLegs",
                            "SystemShipmentLegID",
                            "SystemLoadID",
                            "ShipmentSequenceNumber",
                            "ShipFromLocationCode",
                            "ShipToLocationCode",
                            "Skids"
                        ],
                        "collection": [
                            {
                                "name": "referenceNumberStructure"
                            },
                            {
                                "name": "Shipment.ShipmentLeg",
                                "select": {
                                    "name": [
                                        "SystemShipmentLegID",
                                        "ShipmentNumber",
                                        "ShipToAddress",
                                        "ShipFromAddress",
                                        "SystemLoadID",
                                        "ShipmentSequenceNumber",
                                        "ShipFromLocationCode",
                                        "ShipToLocationCode",
                                        "ShipToLocationName",
                                        "ShipFromLocationName",
                                        "ComputedDateTimeOfPickArrival",
                                        "ComputedDateTimeOfPickDeparture",
                                        "ComputedDateTimeOfDropArrival",
                                        "ComputedDateTimeOfDropDeparture",
                                        "TransitModeEnumVal",
                                        "Load.Vessel",
                                        "Load.TrailerNumber",
                                        "Load.TractorNumber",
                                        "Load.EquipmentTypeCode",
                                        "Load.TripEntityTypeEnumVal",
                                        "Load.SystemTripID",
                                        "Load.CreatedDateTime",
                                        "Load.DriverCode",
                                        "Load.TotalNumberOfShipments",
                                        "Load.CarrierCode",
                                        "Load.ServiceCode",
                                        "CarrierCode",
                                        "ServiceCode"
                                    ],
                                    "collection": [
                                        {
                                            "name": "Load.referenceNumberStructure"
                                        }
                                    ]
                                }
                            },
                            {
                                "name": "Shipment.Container",
                                "select": {
                                    "name": [
                                        "Id",
                                        "SystemContainerID",
                                        "Quantity",
                                        "ItemNumber",
                                        "ContainerShippingInformation.Volume",
                                        "ContainerShippingInformation.NominalWeight",
                                        "ContainerShippingInformation.Pieces",
                                        "ContainerShippingInformation.Skids",
                                        "TransportOrderInfo.SystemTransportOrderID",
                                        "TransportOrderInfo.OrderNumberCode",
                                        "TransportOrderInfo.OrderLineNumberCode",
                                        "TransportOrderInfo.OrderConsolidationGroupID",
                                        "TransportOrderInfo.ProductNumberCode",
                                        "TransportOrderInfo.ShippingInformation"
                                    ],
                                    "collection": [
                                        {
                                            "name": "ReferenceNumberStructure"
                                        },
                                        {
                                            "name": "WeightByFreightClass"
                                        },
                                        {
                                            "name": "ShipmentItem",
                                            "select": {
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
                                "name": "Shipment.ReferenceNumberStructure",
                                "select": {
                                    "name": [
                                        "SystemReferenceNumberID",
                                        "ReferenceNumberTypeCode",
                                        "ReferenceNumberTypeCodeDescr",
                                        "ReferenceNumber"
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "name": "Stop",
                    "select": {
                        "name": [
                            "Id",
                            "SystemStopID",
                            "StopSequenceNumber",
                            "CountOfShipmentsPickedAtStop",
                            "CountOfShipmentsDroppedAtStop",
                            "ShippingLocationTypeEnumVal",
                            "ShippingLocationTypeEnumDescr",
                            "PickConfirmedFlag",
                            "Address",
                            "stopPlanningStatusEnumVal",
                            "ShippingLocationCode",
                            "PrimaryShippingLocationCode",
                            "PrimaryShippingLocationTypeEnumVal",
                            "PrimaryShippingLocationTypeEnumDescr",
                            "WeekendHolidayBreakHours",
                            "WeekendHolidayBreakOrientationEnumVal",
                            "LoadingInstructionExistFlag",
                            "PickedWeight",
                            "PickedVolume",
                            "PickedOrderValue",
                            "PickedDeclaredValue",
                            "PickedNominalWeight",
                            "PickedTareWeight",
                            "PickedPieces",
                            "PickedSkids",
                            "PickedLadenLength",
                            "DroppedWeight",
                            "DroppedVolume",
                            "DroppedOrderValue",
                            "DroppedDeclaredValue",
                            "DroppedNominalWeight",
                            "DroppedTareWeight",
                            "DroppedPieces",
                            "DroppedSkids",
                            "DroppedLadenLength",
                            "DistanceFromPreviousStop",
                            "TransitTimeFromPreviousStop",
                            "WaitingHours",
                            "LoadingHours",
                            "UnloadingHours",
                            "CustomerCode",
                            "CustomerName",
                            "DockScheduleStatusEnumVal",
                            "DockScheduleStatusEnumDescr",
                            "ShippingLocationName",
                            "AppointmentRequiredCounter",
                            "LastComputedArrivalDateTime",
                            "LastComputedDepartureDateTime",
                            "StopStatusEnumVal",
                            "StopStatusEnumDescr",
                            "Appointment",
                            "DeliveryArrivalDateTime"
                        ],
                        "collection": [
                            {
                                "name": "PickShipmentLegID"
                            },
                            {
                                "name": "PickShipmentLegID"
                            },
                            {
                                "name": "ReferenceNumberStructure"
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
                            {
                                "name": "SystemLoadID"
                            },
                            *load_number
                        ]
                    }
                }
            ]
        }
    }

    Logger.debug("Load type enrichment API request body created successfully.", load_api_request,
                 "Message hdr info: ", message_hdr_info)

    Logger.debug("Exiting get_load_type_enrichment_api_request_body_for_WON function.", message_hdr_info)
    return load_api_request # Return the load type enrichment API request body.


def get_shipment_type_enrichment_api_request_body_for_WON(payload, enriched_message, message_hdr_info=None):
    """
    Creates a shipment type enrichment API request body for a Warehousing Outbound Notification (WON).

    :param payload: AEHMessage/KafkaMessage - The incoming message containing relevant data.
    :param enriched_message: Enriched messages - Enriched data for the payload.
    :param message_hdr_info: Optional. Additional header information for the message.

    :returns:
        dict: A dictionary representing the API request body for shipment type enrichment.
    """
    # Initialize message header info if not provided.
    if message_hdr_info is None:
        message_hdr_info = {}

    Logger.debug("Starting get_shipment_type_enrichment_api_request_body_for_WON function.", message_hdr_info)

    # Check if the payload is of type AEHMessage or KafkaMessage and extract its value.
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    # Define an expression to extract shipment IDs from the payload.
    expression = 'warehousingOutboundNotification[].shipment[].transactionalReference[].entityId'

    def _get_shipment_enrichment_api_call(payload, enriched_message, expression):
        # Extract unique shipment IDs from the payload.
        shipment_id_list= set(jmespath.search(expression,payload))
        shipment_id = [{"value": id} for id in shipment_id_list]

        # Define the structure of the shipment type enrichment API request body.
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
                },{
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

    Logger.debug("Exiting get_shipment_type_enrichment_api_request_body_for_WON function.", message_hdr_info)
    # Call the inner function to get the shipment type enrichment API request body.
    return _get_shipment_enrichment_api_call(payload, enriched_message, expression)
