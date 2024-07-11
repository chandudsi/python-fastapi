from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
import json
import copy
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
import json


def get_itemMasterDetails(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    item_number = payload["item"][0]["itemId"]["primaryId"]
    item_master_api_call = {
    "select": {
        "name": [
            "itemNumber",
            "itemDescription",
            "nominalWeight",
            "itemGroupCode"
        ]
    },
    "condition": {
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
                                                    "name": "itemNumber"
                                                },
                                                {
                                                    "value": item_number
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
    return item_master_api_call if len(item_number) > 0 else None

def get_multiple_itemPackage_level_condtion_data(message):

    response = []
    for message_obj in message:
        itemNumber = message_obj["itemNumber"]
        itemGroupCode = message_obj["itemGroupCode"]

        response.append(
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
                                                    "name": "itemNumber"
                                                },
                                                {
                                                    "value": itemNumber
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "eq": {
                                            "o": [
                                                {
                                                    "name": "itemGroupCode"
                                                },
                                                {
                                                    "value": itemGroupCode
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
    return response

def get_itemPackageLevel(payload, enriched_message):
    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value
    enriched_data = enriched_message["itemMaster"]

    itemPackageLevel_data = get_multiple_itemPackage_level_condtion_data(enriched_data)

    item_packageLevel_api_call = {
        "select": {
            "name": [
                "itemNumber",
                "itemGroupCode",
                "ratingUnitsForContainerEnumVal",
                "contentLevelTypeCode",
                "itemPackageLevelIDCode"
            ]
        },
        "condition": {
            "o": [
                {
                    "or": {
                        "o": itemPackageLevel_data
                    }
                }
            ]
        }
    }
    return item_packageLevel_api_call if len(item_packageLevel_api_call) > 0 else None


def get_updated_message_post_enrichment_call(entity, cfg, message, trigger, trigger_options, enricher_options,
                                             enriched_messages=None, message_hdr_info={}):
    resultList = []
    is_3PL_falg = adapter_cps_provider.get_properties().get("3pl_config")

    msg_value = message
    if isinstance(message, AEHMessage) or isinstance(message, Message):
        msg_value = json.loads(message.value)
    enriched_input_message = msg_value["item"]
    # Create new dictionaries based on the first element of enriched_input_message
    new_objects = [copy.deepcopy(enriched_input_message[0]) for _ in range(len(enriched_messages["itemMaster"]))]
    itemPackageLevelGroupIdList = []
    if enriched_messages["itemPackageLevel"] is not None:
        for itemPackageLevelGroupList_ in enriched_messages["itemPackageLevel"]:
            if itemPackageLevelGroupList_["itemPackageLevelIDCode"] == "300":
                itemPackageLevelGroupIdList.append(itemPackageLevelGroupList_["itemGroupCode"])
    # Iterate through the new dictionaries and update the "ownerOfTradeItem" field
    for new_obj, itemMaster_obj in zip(new_objects, enriched_messages["itemMaster"]):
        if is_3PL_falg:
            new_obj["create_flag"] = False
        if itemMaster_obj["itemGroupCode"] in itemPackageLevelGroupIdList:
            new_obj["create_flag"] = False
        if itemMaster_obj["itemGroupCode"] not in itemPackageLevelGroupIdList and not is_3PL_falg:
            new_obj["create_flag"] = True

        new_obj["ownerOfTradeItem"]["primaryId"] = itemMaster_obj["itemGroupCode"]

    # Extend resultList with the new dictionaries
    resultList.extend(new_objects)

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val.update({'enrichment': enriched_messages})
        message_val.update({'item': resultList})
        message_val.update({'incoming_item': enriched_input_message})
        message.value = message_val
    else:
        message.update({'enrichment': enriched_messages})
        message.update({'item': resultList})
        message.update({'incoming_item': enriched_input_message})

    return None, None