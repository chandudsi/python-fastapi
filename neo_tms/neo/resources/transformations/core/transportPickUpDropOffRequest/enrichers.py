import traceback
from neo.log import Logger
from neo.connect.cps import adapter_cps_provider 

def validate_transportPickupDropoffRequest_tms_payload(entity, cfg, message, trigger, trigger_options, enricher_options,
                                               enriched_messages, message_hdr_info, *args, **kwargs):
    """
    This function is used to validate if the required fields are missing
    :param payload:
    :return:
    """
    if not message.get("CISDocument", {}).get("DockCommitmentID"):
        ERROR_MSG = "Missing dockCommitmentID for IffTransportPickUpDropOffExtension"
        Logger.error(f"Enrichment Failed due to {ERROR_MSG}, \n {traceback.format_exc()}, message_header: {message_hdr_info}")
        raise ValueError(ERROR_MSG)
    if not message.get("CISDocument", {}).get("Dock", {}).get("DockID"):
        ERROR_MSG = "Missing dockID IffTransportPickUpDropOffExtension"
        Logger.error(f"Enrichment Failed due to {ERROR_MSG}, \n {traceback.format_exc()}, message_header: {message_hdr_info}")
        raise ValueError(ERROR_MSG)
    return (message, None)

def form_transportPickupDropoffRequest_tms_payload(entity, cfg, complete_payload, trigger, trigger_options, enricher_options, enriched_messages, message_hdr_info):
    """
    This function used to combine 2 input values
    1. Event input from TMS
    2. Enrichment API response from TMS
    :param payload:
    :return:
    """
    try:
        TM_STOP_ENUM_VAL = adapter_cps_provider.get_properties()['codMapConfig']["TM_STOP_ENUM_VAL"]
        TM_APPT_ENUM_VAL = adapter_cps_provider.get_properties()['codMapConfig']["TM_APPT_ENUM_VAL"]
    except KeyError as e:
        ERR_MSG = "Enum values missing from code-map for TM_STOP_ENUM_VAL or TM_APPT_ENUM_VAL."
        Logger.error(ERR_MSG)
        raise KeyError(ERR_MSG)
    # ---
    # Get dock-id
    DOCK_ID = complete_payload.get("CISDocument", {}).get("Dock", {}).get("DockID")
    # From info of all dock get the relevant dock
    try:
        tms_api_data = next(dock for dock in enriched_messages['tms_api_data'] if dock.get('dockCode') == DOCK_ID)
    except StopIteration:
        err_msg = f"No matching dock found in TMS API data for the specified DOCK_ID: {DOCK_ID}."
        Logger.error(err_msg)
        raise IndexError(err_msg)
    dock_appointment_type = ""
    try:
        dock_commitments = tms_api_data.get("dockCommitment", [])[0]["load"]["stop"]
        if dock_commitments:
            dock_commitment_info = dock_commitments[0]
            dock_appointment_type = dock_commitment_info.get("stopTypeEnumVal")
        else:
            raise IndexError("Dock commitment information not found.")
    except Exception as e:
        err_msg = f"An error occurred while retrieving dock commitment information: {str(e)}"
        Logger.warn(err_msg)
    dock_stoptypeenumvalcode = tms_api_data.get("dockTypeEnumVal", "")
    complete_payload["dockTypeEnumVal"] = dock_stoptypeenumvalcode
    complete_payload["systemDockID"] = tms_api_data.get("systemDockID")
    dock_stoptypeenumvalcode = TM_APPT_ENUM_VAL.get(dock_appointment_type, TM_STOP_ENUM_VAL.get(dock_stoptypeenumvalcode, "PICK_UP"))
    complete_payload["ext_dock_stoptypeenumvalcode"] = dock_stoptypeenumvalcode
    start_datetime = complete_payload.get("CISDocument", {}).get("DockCommitmentStartDateTime")
    end_datetime = complete_payload.get("CISDocument", {}).get("DockCommitmentEndDateTime")
    complete_payload["ext_timeZoneOffset"] = tms_api_data.get("businessHours", {}).get("timeZoneOffset")

    if complete_payload.get("CISDocument", {}).get("EventName") != "DockCommitmentDeleted" and dock_stoptypeenumvalcode == "PICK_UP":
        complete_payload["ext_transportEquipment_value"] = tms_api_data.get("EquipmentTypeCode")
        additionalLocationId = complete_payload.get("CISDocument", {}).get("Dock",
                                                                                                        {}).get(
            "DockShippingLocationID", "")
        complete_payload["ext_pickup_additionalLocationId"] = [additionalLocationId] if additionalLocationId else []
        complete_payload["ext_pickup_sublocationId"] = tms_api_data.get("dockSlotIdentifier")
        complete_payload["ext_pickup_language"] = "en"
        complete_payload["ext_pickup_locationSpecificInstructions_value"] = tms_api_data.get("inboundOutboundEnvironmentEnumVal")

        complete_payload["ext_pickup_beginDate"] = start_datetime.split("T")[0]#.replace("-", "")
        complete_payload["ext_pickup_beginTime"] = start_datetime.split("T")[1]
        complete_payload["ext_pickup_endDate"] = end_datetime.split("T")[0]#.replace("-", "")
        complete_payload["ext_pickup_endTime"] = end_datetime.split("T")[1]

    if complete_payload.get("CISDocument", {}).get("EventName") != "DockCommitmentDeleted" and dock_stoptypeenumvalcode == "DROP_OFF":
        complete_payload["ext_transportEquipment_value"] = tms_api_data.get("EquipmentTypeCode")
        additionalLocationId = complete_payload.get("CISDocument", {}).get("Dock",{}).get("DockShippingLocationID")
        complete_payload["ext_dropoff_additionalLocationId"] = [additionalLocationId] if additionalLocationId else []
        complete_payload["ext_dropoff_sublocationId"] = tms_api_data.get("dockSlotIdentifier")
        complete_payload["ext_dropoff_language"] = "en"
        complete_payload["ext_dropoff_locationSpecificInstructions_value"] = tms_api_data.get("inboundOutboundEnvironmentEnumVal")

        complete_payload["ext_dropoff_beginDate"] = start_datetime.split("T")[0]#.replace("-", "")
        complete_payload["ext_dropoff_beginTime"] = start_datetime.split("T")[1]
        complete_payload["ext_dropoff_endDate"] = end_datetime.split("T")[0]#.replace("-", "")
        complete_payload["ext_dropoff_endTime"] = end_datetime.split("T")[1]

    return complete_payload, complete_payload

def transportpickupdropoffRequest_tms_payload(payload, *args, **kwargs):
    if payload.get("CISDocument", {}).get("SystemLoadID", ""):
        query_param_name = "load.systemLoadID"
        query_param_value = payload.get("CISDocument", {}).get("SystemLoadID", "")
    else:
        query_param_name = "systemDockCommitmentID"
        query_param_value = payload.get("CISDocument", {}).get("DockCommitmentID", "")

    ShippingLocationCode = (
        payload.get("CISDocument", {}).get("Dock", {}).get("DockShippingLocationID")
    )
    DockCode = payload.get("CISDocument", {}).get("Dock", {}).get("DockID")
    ShippingLocationCode = (
        payload.get("CISDocument", {}).get("Dock", {}).get("DockShippingLocationID")
    )
    # Since the TMS API exclusively recognizes Enum-val, which is absent in the received event,
    # Therefore, appending a prefix to DockShippingLocationType
    shippingLocationTypeEnumVal = 'SPT_'+(
        payload.get("CISDocument", {}).get("Dock", {}).get("DockShippingLocationType")
    )

    request_payload = {
        "select": {
            "name": [
                "systemDockID",
                "dockCode",
                "dockTypeEnumVal",
                "shippingLocationCode",
                "shippingLocationTypeEnumVal",
                "inboundOutboundEnvironmentEnumVal",
                "businessHours.TimeZoneOffset",
                "dockSlotIdentifier",
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
                                                    {"name": query_param_name},
                                                    {"value": query_param_value},
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    },
                    "select": {
                        "name": [
                            "systemDockCommitmentID",
                            "systemDockID",
                            "commitmentStartDateTime",
                            "commitmentEndDateTime",
                            "carrierCode",
                            "load.systemLoadID",
                            "load.currentLoadOperationalStatusEnumVal",
                            "load.trailerNumber",
                            "load.equipmentTypeCode",
                        ],
                        "collection": [
                            {
                                "name": "load.stop",
                                "condition": {
                                    "o": [
                                        {
                                            "or": {
                                                "o": [
                                                    {
                                                        "eq": {
                                                            "o": [
                                                                {
                                                                    "name": "shippingLocationCode"
                                                                },
                                                                {
                                                                    "value": ShippingLocationCode
                                                                },
                                                            ]
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                },
                                "select": {
                                    "name": [
                                        "systemStopID",
                                        "stopTypeEnumVal",
                                        "countOfShipmentsPickedAtStop",
                                        "countOfShipmentsDroppedAtStop",
                                    ]
                                },
                            }
                        ],
                    },
                }
            ],
        },
        "filter": [
            {"op": "EqOrNull", "name": "dockCode", "value": [DockCode]},
            {
                "op": "EqOrNull",
                "name": "shippingLocationCode",
                "value": [ShippingLocationCode],
            },
            {
                "op": "EqOrNull",
                "name": "shippingLocationTypeEnumVal",
                "value": [shippingLocationTypeEnumVal],
            }
        ],
    }
    return request_payload
