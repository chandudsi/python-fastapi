import neo.process.transformation.utils.operation_util as operation_utils
import copy
from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger
from neo.log import Logger # pragma: no cover
import neo.process.transformation.utils.operation_util as operation_utils

look_up_dict = adapter_cps_provider.get_properties().get("codMapConfig")

def get_orientation_mapping(*args):
    # This function is used to map the containerOrientation value
    containerOrientation = args[0][0]
    containerOrientation_response = []
    if containerOrientation not in [None, '', []]:
        for containerOrientation_object in containerOrientation:
            containerOrientation_response.append({"containerOrientationEnumVal": getattr(containerOrientation_object, "containerOrientationType"), "orientationAllowedEnumVal": getattr(containerOrientation_object, "orientationAllowed")})

    return containerOrientation_response

def get_item_group_uom_conversion(*args):
    # This function is used for uom conversion of length, width and volume for itemPackageLevel
    grossValue = args[0][0]
    primaryId = args[0][1]
    category = args[0][2]

    source_unit = getattr(grossValue, "measurementUnitCode")
    value = getattr(grossValue, "value")
    uom_category = "itemgroup_UOM"
    if primaryId == "----":
        primaryId = "DEFAULT"
    target_unit = adapter_cps_provider.get_properties()[uom_category][primaryId]["target_unit"][category]
    calculated_value = operation_utils.OperationUtils.getUomConversion(category, source_unit, target_unit, value)
    return calculated_value

def get_itemGroupCode_mapping(*args):
    # This funtion is used to map the itemGroupCode from enrichment
    itemgroupId_enrichment = args[0][0]
    itemgroupId_without_enrichment = args[0][1]
    is_3PL_enabled = args[0][2]
    # itemPackageLevelGroupIdList = []

    if is_3PL_enabled:
        return itemgroupId_without_enrichment
    return itemgroupId_enrichment

def is_itemPackage_level_update(*args):
    itemgroupId_enrichment = args[0][0]
    itemgroupId_without_enrichment = args[0][1]
    is_3PL_enabled = args[0][2]
    create_flag = args[0][3]
    if not create_flag:
        if is_3PL_enabled:
            if itemgroupId_enrichment == itemgroupId_without_enrichment:
                return True
            else:
                return False
        else:
            return True
    else:
        return False





