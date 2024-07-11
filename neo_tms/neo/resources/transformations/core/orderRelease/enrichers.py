# orderRelease Enrichers Start
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
import json
from neo.log import Logger as logger


def get_entity_specific_item_number(payload):
    item_number = []
    if payload.get('orderRelease') is not None:
        for order in payload.get("orderRelease", []):
            if order.get("documentActionCode") =="ADD":
                for line_item in order.get("lineItem"):
                    individual_line_number = line_item.get("transactionalTradeItem", {}).get('primaryId')
                    transactionalItemData = get_value(line_item, "transactionalTradeItem.transactionalItemData", {})
                    transactionalItemWeight_Value = get_value(transactionalItemData, "transactionalItemWeight.measurementValue", {})
                    Weight_value = transactionalItemWeight_Value.get("value", None)
                    if line_item.get("freightClassCode", None) is None or Weight_value is None:
                        item_number.append({
                            "and": {
                                "o": [
                                    {
                                        "eq": {
                                            "o": [
                                                {"name": "ItemNumber"}, {"value": individual_line_number}
                                            ]
                                        }
                                    }
                                ]
                            }
                        })

    else:
        pass
    return item_number

def get_item_master_enrichment_request_body(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value


    item_number = get_entity_specific_item_number(payload)

    if len(item_number) == 0:
        return None
    item_master_api_call = {
        "select": {
            "name": ["ItemNumber","NominalWeight","freightClassCode"]
        },
        "condition": {
            "o": [
                {
                    "or": {
                        "o": [
                            *item_number
                        ]
                    }
                }
            ]
        }
    }
    return item_master_api_call if len(item_number) > 0 else None

def get_entity_specific_shipping_location(payload):
    shipping_location = []
    if payload.get('orderRelease') is not None:
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
        if order.get("documentActionCode") not in ["DELETE", "CANCEL"]:
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
    if payload.get("orderRelease", [{}])[0].get("documentActionCode") == "ADD":
        return None
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
                            "Length", "Height", "Width", "ContainerTypeCode", "ContainerShippingInformation",
                            "itemDescription"
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

    # check to get sender system either HOST or WM
    sender_system = None
    try:
        sender_system = list_of_order[0].get('senderSystem')
    except:
        pass
    do_prechecks_on_order_list_tms(list_of_order)

    if sender_system is None:
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
                "ship_to_location_code": order.get("orderLogisticalInformation", {}).get("shipTo", {}).get("primaryId"),
                "ship_from_additional_party_identification": order.get("orderLogisticalInformation", {}).get(
                    "shipFrom", {}).get("primaryId"),
                "requested_ship_date_range_begin_date": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedShipDateRange", {}).get("beginDate"),
                "requested_ship_date_range_begin_time": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedShipDateRange", {}).get("beginTime"),
                "requested_ship_date_range_end_date": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedShipDateRange", {}).get("endDate"),
                "requested_ship_date_range_end_time": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedShipDateRange", {}).get("endTime"),
                "requested_delivery_date_range_begin_date": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedDeliveryDateRange", {}).get("beginDate"),
                "requested_delivery_date_range_begin_time": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedDeliveryDateRange", {}).get("beginTime"),
                "requested_delivery_date_range_end_date": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedDeliveryDateRange", {}).get("endDate"),
                "requested_delivery_date_range_end_time": order.get("orderLogisticalInformation", {}).get(
                    "orderLogisticalDateInformation", {}).get("requestedDeliveryDateRange", {}).get("endTime")
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


def get_value(dict_obj, attribute, default= None):
    item = dict_obj
    attr_list = attribute.split('.')

    if item is None:
        return default
    for attr in attr_list:
        if isinstance(item, list):
            item = item[0]
        if attr in item:
            item = item.get(attr, None)
        else:
            return default
    return item