from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger
import inspect

look_up_dict = adapter_cps_provider.get_properties().get("3pl_config")


def check_item_partial_change(*args):
    item_documentActionCode = args[0][0]
    itemLogisticUnit = args[0][1]
    message_hdr = args[0][2]

    if item_documentActionCode == "PARTIAL_CHANGE":
        for itemLogisticUnit_object in itemLogisticUnit:
            if getattr(itemLogisticUnit_object, "logisticUnitName") == "EA":
                logger.info(f"Item Package Level is enabled", message_hdr)
                return True
    return False

