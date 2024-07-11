from neo.log import Logger as logger
from neo.process.transformation.utils.operation_util import OperationUtils
from collections import defaultdict


"""Generate Random using uuid and convert UUID object to string"""
def generate_uuid(*args,**kargs):
    from uuid import uuid4
    return str(uuid4())

"""Voucher message version"""
def get_voucher_messageVersion(args=None):

    if args is None:
        return None

    return args[0]

def get_current_timestamp_iso8601(*args,**kargs):
    from pendulum import now
    return now().isoformat(timespec='milliseconds')

def getVoucherSubTypeFromClassId(args=None):

    if args in [None,'']:
        return None


    if str(args[0]) == "3407":
        return "INITIAL"
    elif str(args[0]) == "3408":
        return "POST_CHARGE"
    elif str(args[0]) == "3409":
        return "MISCELLANEOUS"

def getVoucherChargeLevelCode(chargeLevelEnumVal=None):

    if chargeLevelEnumVal in [None, '']:
        return None

    if chargeLevelEnumVal[0] == "CHL_NULL":
        return "NULL"
    elif chargeLevelEnumVal[0] == "CHL_SERVICE":
        return "SERVICE"
    elif chargeLevelEnumVal[0] == "CHL_CONDITION":
        return "MANDATORY"
    elif chargeLevelEnumVal[0] == "CHL_OPTION":
        return "OPTIONAL"
    elif chargeLevelEnumVal[0] == "CHL_FEDLTAX":
        return "FEDERAL_TAX"
    elif chargeLevelEnumVal[0] == "CHL_PROVTAX":
        return "PROVINCIAL_TAX"
    elif chargeLevelEnumVal[0] == "CHL_LOCTAX":
        return "LOCAL_TAX"
    elif chargeLevelEnumVal[0] == "CHL_TOTAL":
        return "TOTAL"
    elif chargeLevelEnumVal[0] == "CHL_CONSOLIDATION":
        return "CONSOLIDATION"

def getVoucherChargeDetailType(chargeDetailEnumVal=None):

    if chargeDetailEnumVal in [None, '']:
        return None

    if chargeDetailEnumVal[0] == "CDT_NULL":
        return "NULL"
    elif chargeDetailEnumVal[0] == "CDT_ORIGINAL":
        return "ORIGINAL"
    elif chargeDetailEnumVal[0] == "CDT_ADJUSTMENT":
        return "ADJUSTMENT"
    elif chargeDetailEnumVal[0] == "CDT_ADJUSTMENT_CONTINUOUS_MOVE":
        return "ADJUSTMENT_CONTINUOUS_MOVE"
    elif chargeDetailEnumVal[0] == "CDT_ADJUSTMENT_SYSTEM":
        return "ADJUSTMENT_SYSTEM"
    elif chargeDetailEnumVal[0] == "CDT_ADJUSTMENT_CONSOLIDATION":
        return "ADJUSTMENT_CONSOLIDATION"
    elif chargeDetailEnumVal[0] == "CDT_POST_CHARGE":
        return "POST_CHARGE"
    elif chargeDetailEnumVal[0] == "CDT_MISC_CHARGE":
        return "MISCELLANEOUS"
    elif chargeDetailEnumVal[0] == "CDT_CARR_SCHG_BASE":
        return "SURCHARGE_BASE"
    elif chargeDetailEnumVal[0] == "CDT_CARR_SCHG_PERCENT":
        return "SURCHARGE_PERCENTAGE"
    elif chargeDetailEnumVal[0] == "CDT_CARR_SCHG_AMOUNT":
        return "SURCHARGE_AMOUNT"
    elif chargeDetailEnumVal[0] == "CDT_BALANCE_DUE":
        return "BALANCE_DUE"

def getVoucherChargeStatusCode(currentStatusEnumVal=None):

    if currentStatusEnumVal in [None, '']:
        return None

    if currentStatusEnumVal[0] == "CDS_NULL":
        return "NULL"
    elif currentStatusEnumVal[0] == "CDS_INELIGIBLE":
        return "INELIGIBLE"
    elif currentStatusEnumVal[0] == "CDS_ACCRUAL_PENDING":
        return "ACCRUAL_PENDING"
    elif currentStatusEnumVal[0] == "CDS_ACCRUED":
        return "ACCRUED"
    elif currentStatusEnumVal[0] == "CDS_ACCRUAL_REVERSED":
        return "ACCRUAL_REVERSED"
    elif currentStatusEnumVal[0] == "CDS_POSTED":
        return ""
    elif currentStatusEnumVal[0] == "CDS_CANCELED":
        return "CANCELLED"
    elif currentStatusEnumVal[0] == "CDS_CLOSED":
        return "CLOSED"

def getVoucherStatusCode(currentStatusEnumVal=None):

    if currentStatusEnumVal in [None, '']:
        return None

    if currentStatusEnumVal[0] == "APVS_NULL":
        return "NULL"
    elif currentStatusEnumVal[0] == "APVS_INELIGIBLE":
        return "INELIGIBLE"
    elif currentStatusEnumVal[0] == "APVS_ELIGIBLE":
        return "ELIGIBLE"
    elif currentStatusEnumVal[0] == "APVS_MATCHED_VARIANCE":
        return "MATCHED_WITH_VARIANCE"
    elif currentStatusEnumVal[0] == "APVS_MATCHED":
        return "MATCHED"
    elif currentStatusEnumVal[0] == "APVS_CLOSED":
        return "CLOSED"
    elif currentStatusEnumVal[0] == "APVS_CANCELED":
        return "CANCELLED"
def getVoucherLevelCode(apVoucherLevelEnumVal=None):

    if apVoucherLevelEnumVal in [None, '']:
        return None

    if apVoucherLevelEnumVal[0] == "APVL_NULL":
        return "NULL"
    elif apVoucherLevelEnumVal[0] == "APVL_MANIFEST_LD_LEG":
        return "MANIFEST"
    elif apVoucherLevelEnumVal[0] == "APVL_BUILT_LD_LEG":
        return "LOAD"
    elif apVoucherLevelEnumVal[0] == "APVL_MANIFEST_LD_LEG_DETL":
        return "SHIPMENT_LEG"
    elif apVoucherLevelEnumVal[0] == "APVL_MANIFEST_LD_LEG_DETL_COMP":
        return "SHIPMENT_LEG_CONTAINER"
    elif apVoucherLevelEnumVal[0] == "APVL_TRIP":
        return "CONTINUOUS_MOVE"
    elif apVoucherLevelEnumVal[0] == "APVL_BOOKING":
        return "BOOKING"
    elif apVoucherLevelEnumVal[0] == "APVL_NON_OP_FRHT_AR":
        return "NON_OPERATIONAL_FREIGHT"

"Convert to float"
def convertToFloat(args):
    if args in [None, '']:
        return None

    return float(args[0])
                