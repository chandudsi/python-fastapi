import json
from datetime import datetime
def validate_transportPickupDropoffConfirmation_tms_payload(entity, cfg, payload, *args, **kwargs):
    """
    This function identifies if it is a pickUp or dropOff, then checks and assigns dock_slot_identifier
    """
    payload = json.loads(payload.value)
    # raise ValueError(payload)
    dock_slot_identifier = ""
    if payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff"):
        dock_slot_identifier = \
            payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff", {}).get(
                "logisticLocation").get("sublocationId")[0]
    elif payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp"):
        dock_slot_identifier = \
            payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp", {}).get(
                "logisticLocation").get("sublocationId")[0]

    return (payload, None)

def get_formatted_time(time_string):
    try:
        tz_info = time_string[-6:]
        dt_object = datetime.strptime(time_string, '%H:%M:%S%z')
        # Extract the time component and format it
        formatted_time = dt_object.strftime('%H:%M:%S.%f')
        return formatted_time+tz_info
    except:
        return None

def form_transportPickUpDropOffConfirmation_tms_payload(entity, cfg, complete_payload, trigger, trigger_options,
                                                        enricher_options, enriched_messages, message_hdr_info):
    """
    This function is adding the pickUp and dropOff enrichment details to be used in transformations
    """
    complete_payload = json.loads(complete_payload.value)
    # raise ValueError(payload)
    complete_payload.update(enriched_messages['tms_api_data'][0])
    complete_payload["ext_timeZoneOffset"] = enriched_messages['tms_api_data'][0].get("businessHours", {}).get("timeZoneOffset", 0.0)

    if complete_payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp"):
        complete_payload["ext_beginTime_pickup"] = complete_payload.get("transportPickUpDropOffConfirmation", [])[0]\
                                                    .get("plannedPickUp", {}).get("logisticEventPeriod", {})\
                                                    .get("beginTime")
        complete_payload["ext_beginTime_pickup"] = get_formatted_time(complete_payload["ext_beginTime_pickup"])
        complete_payload["ext_endTime_pickup"] = complete_payload.get("transportPickUpDropOffConfirmation", [])[0]\
                                                    .get("plannedPickUp", {}).get("logisticEventPeriod", {})\
                                                    .get("endTime")
        complete_payload["ext_endTime_pickup"] = get_formatted_time(complete_payload["ext_endTime_pickup"])
    elif complete_payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff"):
        complete_payload["ext_beginTime_dropoff"] = complete_payload.get("transportPickUpDropOffConfirmation", [])[0]\
                                                    .get("plannedDropOff", {}).get("logisticEventPeriod", {})\
                                                    .get("beginTime")
        complete_payload["ext_beginTime_dropoff"] = get_formatted_time(complete_payload["ext_beginTime_dropoff"])
        complete_payload["ext_endTime_dropoff"] = complete_payload.get("transportPickUpDropOffConfirmation", [])[0]\
                                                    .get("plannedDropOff", {}).get("logisticEventPeriod", {})\
                                                    .get("endTime")
        complete_payload["ext_endTime_dropoff"] = get_formatted_time(complete_payload["ext_endTime_dropoff"])
    # # d.raise ValueError(complete_payload)
    complete_payload["transportPickUpDropOffConfirmation"] = \
    complete_payload.get("transportPickUpDropOffConfirmation", {})[0]

    return complete_payload, complete_payload


def transportpickupdropoffConfirmation_tms_payload(original_payload, *args, **kwargs):
    payload = json.loads(original_payload.value)
    """
    This function is creating request payload base on pre-defined condition for SystemLoadID and SystemDockCommitmentID

    """
    try:
        if payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp"):
            shippingLocationCode = \
                payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp", {}).get(
                    "logisticLocation",
                    {}).get(
                    "additionalLocationId", [0])[0]

            dockSlotIdentifier = \
                payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedPickUp", {}).get(
                    "logisticLocation",
                    {}).get(
                    "sublocationId", [])
        elif payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff"):
            shippingLocationCode = \
                payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff", {}).get(
                    "logisticLocation",
                    {}).get(
                    "additionalLocationId", [0])[0]

            dockSlotIdentifier = \
                payload.get("transportPickUpDropOffConfirmation", [])[0].get("plannedDropOff", {}).get(
                    "logisticLocation",
                    {}).get(
                    "sublocationId", [])
        request_payload = {
            "select": {
                "name": [
                    "systemDockID",
                    "dockCode",
                    "dockTypeEnumVal",
                    "shippingLocationCode",
                    "shippingLocationTypeEnumVal",
                    "inboundOutboundEnvironmentEnumVal",
                    "dockSlotIdentifier",
                    "businessHours"
                ],
                "collection": [
                    {
                        "name": "dockCommitment",
                        "condition": {
                            "o": [
                                {
                                    "or": {
                                        "o": [
                                            {
                                                "eq": {
                                                    "o": [
                                                        {
                                                            "name": "systemDockCommitmentID"
                                                        },
                                                        {
                                                            "value":
                                                                payload.get("transportPickUpDropOffConfirmation", [])[
                                                                    0].get(
                                                                    "transportPickUpDropOffConfirmationId")
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        if payload.get("transportPickUpDropOffConfirmation", [])[0].get("documentActionCode") != "DELETE":
            request_payload.update({
                "condition": {
                    "o": [
                        {
                            "or": {
                                "o": [
                                    {
                                        "or": {
                                            "o": [
                                                {
                                                    "and": {
                                                        "o": [
                                                            {
                                                                "eq": {
                                                                    "o": [
                                                                        {
                                                                            "name": "shippingLocationCode"
                                                                        },
                                                                        {
                                                                            "value": shippingLocationCode
                                                                        }
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "eq": {
                                                                    "o": [
                                                                        {
                                                                            "name": "dockSlotIdentifier"
                                                                        },
                                                                        {
                                                                            "value": dockSlotIdentifier
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
            )
    except:
        print("additionalLocationId or other required values are missing.")

    return request_payload