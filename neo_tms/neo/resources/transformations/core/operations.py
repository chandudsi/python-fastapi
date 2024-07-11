from neo.process.transformation.utils.conversion_util import ConversionUtils
from neo.process.transformation.utils.operation_util import OperationUtils
import copy
from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger

look_up_dict = adapter_cps_provider.get_properties().get("codMapConfig")


# order release function start this needs to be removed after checking the sanity of it's usages
# def check_default_flags(*args):
#     if args[0][0] is True:
#         return False
#     return True
#
#
# def check_plan_id(*args):
#     ref_struct = args[0][2]
#     loc_enrichment = args[0][1]
#     order_type = 'others'
#     if loc_enrichment not in [None, '']:
#         if len(loc_enrichment) > 1:
#             shipfrom_enrichment = loc_enrichment[0]
#             shipto_enrichment = loc_enrichment[1]
#         else:
#             shipfrom_enrichment = loc_enrichment
#     else:
#         return None
#     if ref_struct not in [None, '']:
#         for ref in ref_struct:
#             if eval('ref.referenceNumberValue') in ['P', 'EP']:
#                 order_type = 'backward'
#     try:
#         try:
#             if order_type == 'backward':
#                 if eval('shipto_enrichment.externalCode1') not in [None, ''] and eval(
#                         'shipto_enrichment.externalCode1') == 'T':
#                     ship = 'T'
#                 else:
#                     ship = 'F'
#         except Exception:
#             ship = 'F'
#         else:
#             try:
#                 if eval('shipfrom_enrichment.externalCode1') not in [None, ''] and eval(
#                         'shipfrom_enrichment.externalCode1') == 'T':
#                     ship = 'T'
#                 else:
#                     ship = 'F'
#             except Exception:
#                 ship = 'F'
#     except Exception:
#         ship = 'F'
#     if ship in ['T', 't']:  # or shipto in ['T', 't']:
#         if args[0][0] == "01":
#             return "1"
#         elif args[0][0] == "02":
#             return "2"
#         return None
#
#
# def pickup_from_date_time_logic(*args):
#     import datetime as dt
#     import pytz
#     from datetime import timedelta
#     from dateutil import tz
#     ref_struct = args[0][0]
#     rdd_beginDate = args[0][1]
#     rdd_endDate = args[0][2]
#     location_enrichment = args[0][3]
#     shipfrom_location = location_enrichment[0]
#     shipto_location = location_enrichment[1]
#     tms_timezone = args[0][4]
#     ship_to_loc_offset = eval('shipto_location.businessHours.timeZoneOffset')
#     ship_from_loc_offset = eval('shipfrom_location.businessHours.timeZoneOffset')
#
#     # Check the dispatch Type Code:
#
#     if ref_struct not in [None, '']:
#         for ref in ref_struct:
#             if eval('ref.referenceNumberValue') == 'R':
#                 dispatch_type_code = 'R'
#             if eval('ref.referenceNumberValue') == 'D':
#                 dispatch_type_code = 'D'
#             if eval('ref.referenceNumberValue') == 'P':
#                 dispatch_type_code = 'P'
#             if eval('ref.referenceNumberValue') == 'ED':
#                 dispatch_type_code = 'ED'
#             if eval('ref.referenceNumberValue') == 'EP':
#                 dispatch_type_code = 'EP'
#             if eval('ref.referenceNumberValue') == 'T':
#                 dispatch_type_code = 'T'
#
#     # Converts the given time into its respective TimeZone
#     input_format = "%Y-%m-%d"  # T%H:%M:%S%z
#     output_format = "%Y-%m-%dT%H:%M:%S%z"
#     default = 'False'
#
#     if dispatch_type_code not in ['EP', 'P', 'T']:  # forward orders - Transfer Order
#         default_date_time_format = {
#             "input_format": "%Y-%m-%d",  # T%H:%M:%S%z
#             "output_format": "%Y-%m-%d",  # T%H:%M:%S%z
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_from_loc_offset}
#     else:
#         default_date_time_format = {
#             "input_format": "%Y-%m-%d",  # T%H:%M:%S%z
#             "output_format": "%Y-%m-%d",  # T%H:%M:%S%z
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_to_loc_offset}
#
#     props_default_datetime = default_date_time_format
#     if rdd_endDate in [None, ""]:
#         is_default_required = True if default.lower() in ["true"] else False
#         if is_default_required:
#             from pendulum import now
#             return now().to_iso8601_string()
#         else:
#             return None
#     input_format = input_format if (input_format is not None) else props_default_datetime.get("input_format")
#     output_format = output_format if (output_format is not None) else props_default_datetime.get("output_format")
#     tm_server_timezone = props_default_datetime.get("tm_server_timezone")
#     if '/' in tm_server_timezone:
#         timezone_py = pytz.timezone(tm_server_timezone)
#     offset = props_default_datetime.get("offset")
#     datetime_object_rdd_beginDate = dt.datetime.strptime(rdd_beginDate,
#                                                          input_format)  # the format that OMS has "%Y-%m-%dT%H:%M:%S%z", no need of this line
#     datetime_object_rdd_endDate = dt.datetime.strptime(rdd_endDate, input_format)
#     timezone_py = pytz.timezone(tm_server_timezone)
#     timezone = dt.datetime.now(timezone_py).utcoffset() + timedelta(hours=offset)  # UTC-07:00
#     converted_tz = tz.tzoffset('UTC', timezone)
#     if "%z" in output_format:
#         output_format = output_format.replace("%z", "")
#     datetime_object_rdd_beginDate = datetime_object_rdd_beginDate.astimezone(converted_tz)
#     datetime_object_rdd_endDate = datetime_object_rdd_endDate.astimezone(converted_tz)
#     output_beginDate = dt.datetime.strftime(datetime_object_rdd_beginDate, output_format)
#     output_endDate = dt.datetime.strftime(datetime_object_rdd_endDate, output_format)
#
#     # logics to add and subtract time
#
#     if dispatch_type_code in ['R', 'D']:
#         if output_beginDate == output_endDate:
#             return output_endDate.replace(output_endDate[11:19], '00:00:01')
#         pickup_time = datetime_object_rdd_beginDate.min + timedelta(seconds=1)
#         pick_up_to_date_time = datetime_object_rdd_beginDate.combine(datetime_object_rdd_beginDate.date(),
#                                                                      pickup_time.time(),
#                                                                      datetime_object_rdd_beginDate.tzinfo)
#         return dt.datetime.strftime(pick_up_to_date_time, output_format)
#     if dispatch_type_code == 'P':
#         return output_endDate
#     if dispatch_type_code == 'ED':
#         pickup_from_date_time_ed = output_endDate.replace(output_endDate[11:19], '00:00:00')
#         return pickup_from_date_time_ed
#     if dispatch_type_code == 'EP':
#         pickup_from_date_time_ep = datetime_object_rdd_endDate - timedelta(hours=3)
#         return dt.datetime.strftime(pickup_from_date_time_ep, output_format)
#     if dispatch_type_code == 'T':
#         return output_beginDate
#
#
# def pickup_to_date_time_logic(*args):
#     import datetime as dt
#     import pytz
#     from datetime import timedelta
#     from dateutil import tz
#     ref_struct = args[0][0]
#     rdd_beginDate = args[0][1]
#     rdd_endDate = args[0][2]
#     location_enrichment = args[0][3]
#     shipfrom_location = location_enrichment[0]
#     shipto_location = location_enrichment[1]
#     tms_timezone = args[0][4]
#     ship_to_loc_offset = eval('shipto_location.businessHours.timeZoneOffset')
#     ship_from_loc_offset = eval('shipfrom_location.businessHours.timeZoneOffset')
#
#     if ref_struct not in [None, '']:
#         for ref in ref_struct:
#             if eval('ref.referenceNumberValue') == 'R':
#                 dispatch_type_code = 'R'
#             if eval('ref.referenceNumberValue') == 'D':
#                 dispatch_type_code = 'D'
#             if eval('ref.referenceNumberValue') == 'P':
#                 dispatch_type_code = 'P'
#             if eval('ref.referenceNumberValue') == 'ED':
#                 dispatch_type_code = 'ED'
#             if eval('ref.referenceNumberValue') == 'EP':
#                 dispatch_type_code = 'EP'
#             if eval('ref.referenceNumberValue') == 'T':
#                 dispatch_type_code = 'T'
#
#     # Converts the given time into its respective TimeZone
#     input_format = "%Y-%m-%d"  # T%H:%M:%S%z
#     output_format = "%Y-%m-%dT%H:%M:%S%z"
#     default = 'False'
#
#     if dispatch_type_code not in ['EP', 'P']:  # forward orders
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_to_loc_offset}
#     else:
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_from_loc_offset}
#     props_default_datetime = default_date_time_format
#     if rdd_endDate in [None, ""]:
#         is_default_required = True if default.lower() in ["true"] else False
#         if is_default_required:
#             from pendulum import now
#             return now().to_iso8601_string()
#         else:
#             return None
#     input_format = input_format if (input_format is not None) else props_default_datetime.get("input_format")
#     output_format = output_format if (output_format is not None) else props_default_datetime.get("output_format")
#     tm_server_timezone = props_default_datetime.get("tm_server_timezone")
#     if '/' in tm_server_timezone:
#         timezone_py = pytz.timezone(tm_server_timezone)
#     offset = props_default_datetime.get("offset")
#     datetime_object_rdd_beginDate = dt.datetime.strptime(rdd_beginDate,
#                                                          input_format)  # the format that OMS has "%Y-%m-%dT%H:%M:%S%z", no need of this line
#     datetime_object_rdd_endDate = dt.datetime.strptime(rdd_endDate, input_format)
#     timezone_py = pytz.timezone(tm_server_timezone)
#     timezone = dt.datetime.now(timezone_py).utcoffset() + timedelta(hours=offset)  # UTC-07:00
#     converted_tz = tz.tzoffset('UTC', timezone)
#     if "%z" in output_format:
#         output_format = output_format.replace("%z", "")
#     datetime_object_rdd_beginDate = datetime_object_rdd_beginDate.astimezone(converted_tz)
#     datetime_object_rdd_endDate = datetime_object_rdd_endDate.astimezone(converted_tz)
#     output_beginDate = dt.datetime.strftime(datetime_object_rdd_beginDate, output_format)
#     output_endDate = dt.datetime.strftime(datetime_object_rdd_endDate, output_format)
#
#     # logics to add and subtract time
#
#     if dispatch_type_code in ['R', 'D']:
#         pickup_to_date_time = datetime_object_rdd_endDate - timedelta(minutes=1)
#         return dt.datetime.strftime(pickup_to_date_time, output_format)
#
#     if dispatch_type_code == 'P':
#         pick_up_to_date_time = datetime_object_rdd_endDate + timedelta(hours=72)
#         return dt.datetime.strftime(pick_up_to_date_time, output_format)
#
#     if dispatch_type_code == 'ED':
#         pick_up_to_date_time_ed = datetime_object_rdd_endDate - timedelta(minutes=1)
#         return dt.datetime.strftime(pick_up_to_date_time_ed, output_format)
#
#     if dispatch_type_code == 'EP':
#         pickup_from_date_time_ep = datetime_object_rdd_endDate - timedelta(hours=3)
#         pickup_from_date_time_ep_str = dt.datetime.strftime(pickup_from_date_time_ep, output_format)
#         return pickup_from_date_time_ep_str.replace(pickup_from_date_time_ep_str[11:19], '23:58:00')
#
#     if dispatch_type_code == 'T':
#         pickup_from_date_time_t = datetime_object_rdd_endDate - timedelta(minutes=1)
#         return dt.datetime.strftime(pickup_from_date_time_t, output_format)
#
#
# def delivery_from_date_time_logic(*args):
#     import datetime as dt
#     import pytz
#     from datetime import timedelta
#     from dateutil import tz
#     ref_struct = args[0][0]
#     rdd_beginDate = args[0][1]
#     rdd_endDate = args[0][2]
#     location_enrichment = args[0][3]
#     shipfrom_location = location_enrichment[0]
#     shipto_location = location_enrichment[1]
#     tms_timezone = args[0][4]
#     ship_to_loc_offset = eval('shipto_location.businessHours.timeZoneOffset')
#     ship_from_loc_offset = eval('shipfrom_location.businessHours.timeZoneOffset')
#
#     # Check the dispatch Type Code:
#
#     if ref_struct not in [None, '']:
#         for ref in ref_struct:
#             if eval('ref.referenceNumberValue') == 'R':
#                 dispatch_type_code = 'R'
#             if eval('ref.referenceNumberValue') == 'D':
#                 dispatch_type_code = 'D'
#             if eval('ref.referenceNumberValue') == 'P':
#                 dispatch_type_code = 'P'
#             if eval('ref.referenceNumberValue') == 'ED':
#                 dispatch_type_code = 'ED'
#             if eval('ref.referenceNumberValue') == 'EP':
#                 dispatch_type_code = 'EP'
#             if eval('ref.referenceNumberValue') == 'T':
#                 dispatch_type_code = 'T'
#
#     # Converts the given time into its respective TimeZone
#     input_format = "%Y-%m-%d"  # T%H:%M:%S%z
#     output_format = "%Y-%m-%dT%H:%M:%S%z"
#     default = 'False'
#     if dispatch_type_code not in ['EP', 'P', 'T']:  # forward orders - Transfer Order
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_to_loc_offset}
#     else:
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_from_loc_offset}
#     props_default_datetime = default_date_time_format
#     if rdd_endDate in [None, ""]:
#         is_default_required = True if default.lower() in ["true"] else False
#         if is_default_required:
#             from pendulum import now
#             return now().to_iso8601_string()
#         else:
#             return None
#     input_format = input_format if (input_format is not None) else props_default_datetime.get("input_format")
#     output_format = output_format if (output_format is not None) else props_default_datetime.get("output_format")
#     tm_server_timezone = props_default_datetime.get("tm_server_timezone")
#     if '/' in tm_server_timezone:
#         timezone_py = pytz.timezone(tm_server_timezone)
#     offset = props_default_datetime.get("offset")
#     datetime_object_rdd_beginDate = dt.datetime.strptime(rdd_beginDate,
#                                                          input_format)  # the format that OMS has "%Y-%m-%dT%H:%M:%S%z", no need of this line
#     datetime_object_rdd_endDate = dt.datetime.strptime(rdd_endDate, input_format)
#     timezone_py = pytz.timezone(tm_server_timezone)
#     timezone = dt.datetime.now(timezone_py).utcoffset() + timedelta(hours=offset)  # UTC-07:00
#     converted_tz = tz.tzoffset('UTC', timezone)
#     if "%z" in output_format:
#         output_format = output_format.replace("%z", "")
#     datetime_object_rdd_beginDate = datetime_object_rdd_beginDate.astimezone(converted_tz)
#     datetime_object_rdd_endDate = datetime_object_rdd_endDate.astimezone(converted_tz)
#     output_beginDate = dt.datetime.strftime(datetime_object_rdd_beginDate, output_format)
#     output_endDate = dt.datetime.strftime(datetime_object_rdd_endDate, output_format)
#
#     # Logics to add and subtract time
#
#     if dispatch_type_code in ['R', 'D']:
#         if output_beginDate != output_endDate:
#             return output_beginDate
#         else:
#             return output_endDate.replace(output_endDate[11:19], '00:01:01')
#     if dispatch_type_code == 'P':
#         delivery_from_date_time = datetime_object_rdd_endDate + timedelta(minutes=1)
#         return dt.datetime.strftime(delivery_from_date_time, output_format)
#     if dispatch_type_code == 'ED':
#         delivery_from_date_time_ed = datetime_object_rdd_endDate - timedelta(hours=3)
#         return dt.datetime.strftime(delivery_from_date_time_ed, output_format)
#     if dispatch_type_code == 'EP':
#         delivery_from_date_time_ep = datetime_object_rdd_endDate - timedelta(hours=3)
#         delivery_from_date_time_ep_ = delivery_from_date_time_ep + timedelta(minutes=1)  # (change it to -)
#         return dt.datetime.strftime(delivery_from_date_time_ep_, output_format)
#     if dispatch_type_code == 'T':
#         delivery_from_date_time_t = datetime_object_rdd_beginDate + timedelta(minutes=1)
#         return dt.datetime.strftime(delivery_from_date_time_t, output_format)
#
#
# def delivery_to_date_time_logic(*args):
#     import datetime as dt
#     import pytz
#     from datetime import timedelta
#     from dateutil import tz
#     ref_struct = args[0][0]
#     rdd_beginDate = args[0][1]
#     rdd_endDate = args[0][2]
#     location_enrichment = args[0][3]
#     shipfrom_location = location_enrichment[0]
#     shipto_location = location_enrichment[1]
#     tms_timezone = args[0][4]
#     ship_to_loc_offset = eval('shipto_location.businessHours.timeZoneOffset')
#     ship_from_loc_offset = eval('shipfrom_location.businessHours.timeZoneOffset')
#
#     # Check the dispatch Type Code:
#
#     if ref_struct not in [None, '']:
#         for ref in ref_struct:
#             if eval('ref.referenceNumberValue') == 'R':
#                 dispatch_type_code = 'R'
#             if eval('ref.referenceNumberValue') == 'D':
#                 dispatch_type_code = 'D'
#             if eval('ref.referenceNumberValue') == 'P':
#                 dispatch_type_code = 'P'
#             if eval('ref.referenceNumberValue') == 'ED':
#                 dispatch_type_code = 'ED'
#             if eval('ref.referenceNumberValue') == 'EP':
#                 dispatch_type_code = 'EP'
#             if eval('ref.referenceNumberValue') == 'T':
#                 dispatch_type_code = 'T'
#     else:
#         dispatch_type_code = None
#
#     # Converts the given time into its respective TimeZone
#     input_format = "%Y-%m-%d"  # T%H:%M:%S%z
#     output_format = "%Y-%m-%dT%H:%M:%S%z"
#     default = 'False'
#
#     if dispatch_type_code not in ['EP', 'P']:  # forward orders
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_to_loc_offset}
#     else:  # backward orders
#         default_date_time_format = {
#             "input_format": "%Y-%m-%dT%H:%M:%S%z",
#             "output_format": "%Y-%m-%dT%H:%M:%S%z",
#             "tm_server_timezone": tms_timezone,
#             "offset": ship_from_loc_offset}
#
#     props_default_datetime = default_date_time_format
#     if rdd_endDate in [None, ""]:
#         is_default_required = True if default.lower() in ["true"] else False
#         if is_default_required:
#             from pendulum import now
#             return now().to_iso8601_string()
#         else:
#             return None
#     input_format = input_format if (input_format is not None) else props_default_datetime.get("input_format")
#     output_format = output_format if (output_format is not None) else props_default_datetime.get("output_format")
#     tm_server_timezone = props_default_datetime.get("tm_server_timezone")
#     if '/' in tm_server_timezone:
#         timezone_py = pytz.timezone(tm_server_timezone)
#     offset = props_default_datetime.get("offset")
#     datetime_object_rdd_beginDate = dt.datetime.strptime(rdd_beginDate,
#                                                          input_format)  # the format that OMS has "%Y-%m-%dT%H:%M:%S%z", no need of this line
#     datetime_object_rdd_endDate = dt.datetime.strptime(rdd_endDate, input_format)
#     timezone_py = pytz.timezone(tm_server_timezone)
#     timezone = dt.datetime.now(timezone_py).utcoffset() + timedelta(hours=offset)  # UTC-07:00
#     converted_tz = tz.tzoffset('UTC', timezone)
#     if "%z" in output_format:
#         output_format = output_format.replace("%z", "")
#     datetime_object_rdd_beginDate = datetime_object_rdd_beginDate.astimezone(converted_tz)
#     datetime_object_rdd_endDate = datetime_object_rdd_endDate.astimezone(converted_tz)
#     output_beginDate = dt.datetime.strftime(datetime_object_rdd_beginDate, output_format)
#     output_endDate = dt.datetime.strftime(datetime_object_rdd_endDate, output_format)
#
#     # Logics to add and subtract time
#
#     if dispatch_type_code in ['R', 'D', 'ED']:
#         return output_endDate
#     if dispatch_type_code == 'P':
#         del_to_date_time = datetime_object_rdd_endDate + timedelta(hours=72)
#         del_to_date_time_str = dt.datetime.strftime(del_to_date_time, output_format)
#         return del_to_date_time_str.replace(del_to_date_time_str[11:19], '23:59:00')
#     if dispatch_type_code == 'EP':
#         delivery_to_date_time_ep = datetime_object_rdd_endDate - timedelta(hours=3)
#         delivery_to_date_time_ep_str = dt.datetime.strftime(delivery_to_date_time_ep, output_format)
#         return delivery_to_date_time_ep_str.replace(delivery_to_date_time_ep_str[11:19], '23:59:00')
#     if dispatch_type_code == 'T':
#         return output_endDate
#
#
# def round_to_four(*args):
#     if args[0][0] in [None, '']:
#         return None
#     val = round(float(args[0][0]), 4)
#     return val
#
#
# def mul_nominal_weight(*args):
#     if args[0][0] not in [None, ''] and args[0][1] not in [None, '']:
#         return float(args[0][0]) * float(args[0][1])
#     else:
#         return 0
#
#
# def reference_number_structure_tms(*args):
#     reference_number = {}
#     flag = []
#     try:
#         for ref_struct in args[0][0]:
#             flag.append({'referenceNumberTypeCode': eval('ref_struct.referenceNumberName'),
#                          'referenceNumber': eval('ref_struct.referenceNumberValue')})
#         # flag.append({'referenceNumberTypeCode': 'ADESC', 'referenceNumber': args[0][1]})
#         reference_number = {'referenceNumberStructure': flag}
#         return reference_number
#     except:
#         return None
#
#
# def reference_number_structure_container_tms(*args):
#     reference_number = {}
#     lineNumber = args[0][0]
#     reference_number = {
#         'referenceNumberStructure': [
#             {
#                 'referenceNumberTypeCode': 'WM_ORDLIN_',
#                 'referenceNumber': lineNumber
#             },
#             {
#                 'referenceNumberTypeCode': 'WM_ORDSLN_',
#                 'referenceNumber': 0
#             }
#         ]
#     }
#
#     return reference_number
#
#
# # order release function end

# this is an example
def get_pickup_stop_seq(*args):
    in_str = args[0][0]
    if in_str.upper() == "TERMINAL_DEPARTURE":
        return args[0][1]
    else:
        return None


def get_dropoff_stop_seq(*args):
    in_str = args[0][0]
    if in_str.upper() == "TERMINAL_ARRIVAL":
        return args[0][1]
    else:
        return None


def get_pickup_reference(*args):
    in_str = args[0][0]
    if in_str.upper() == "PICKUP":
        return args[0][1]
    else:
        return None


def get_dropoff_reference(*args):
    in_str = args[0][0]
    if in_str.upper() == "DROPOFF":
        return args[0][1]
    else:
        return None


def calculate_date_time_from_epoc(epoc_time):
    """Get date and time of the passed fields"""
    import time
    if isinstance(epoc_time, list):
        epoc_time = epoc_time[0] / 1000
    else:
        epoc_time = epoc_time / 1000
    date_time = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(epoc_time))
    return date_time


def calculate_date_from_epoc(epoc_time):
    """Get only date of the passed date and time field"""
    import time
    if isinstance(epoc_time, list):
        epoc_time = epoc_time[0] / 1000
    else:
        epoc_time = epoc_time / 1000
    date_time = time.strftime('%Y-%m-%d', time.gmtime(epoc_time))
    return date_time


def calculate_time_from_epoc(epoc_time):
    """Get only time of the passed date and time field"""
    import time
    if isinstance(epoc_time, list):
        epoc_time = epoc_time[0] / 1000
    else:
        epoc_time = epoc_time / 1000
    date_time = time.strftime('%H:%M:%S', time.gmtime(epoc_time))
    return date_time


def get_modified_date(dateTimeValue):
    import datetime
    from datetime import datetime
    if dateTimeValue[0] is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue[0], "%Y%m%d%H%M%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def get_modified_timeStamp(dateTimeValue):
    import datetime
    from datetime import datetime
    if dateTimeValue[0] is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue[0], "%Y%m%d%H%M%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%dT%H:%M:%SZ")
    return date


def generate_uuid(*args, **kwargs):
    from uuid import uuid4
    return str(uuid4())


def generate_uuid_BYDM(*args, **kwargs):
    from uuid import uuid4
    return ('BYDM' + str(uuid4()))


def get_current_timestamp_iso8601(*args, **kwargs):
    from pendulum import now
    return now().isoformat(timespec='milliseconds')


def custom_function_1(*args):
    if args[0] is None:
        return None
    else:
        return {
            "value": args[0]
        }


def get_day(day):
    return day[0]


def get_date(date):
    return "%s-%s-%s" % (date[0].values[0], date[1].values[0], date[2].values[0])


def get_boolean_value(value):
    return value == 1


def get_boolean(*args):
    # if args[0][0] == "True" or args[0][0] == '1':
    if args[0][0] is True or args[0][0] == 1:
        return True
    return False


def get_action_code(*args):
    action_code = {
        "INSERT": "CREATE",
        "UPDATE": "PATCH",
        "DELETE": "DELETE"
    }
    if args[0][0] is None:
        return None
    return action_code[args[0][0]]


def get_bydm_action_code(*arg):
    action_code = {
        "INSERT": "ADD",
        "UPDATE": "CHANGE_BY_REFRESH",
        "DELETE": "DELETE"
    }
    return action_code.get(*arg)


def append_stacking_rule(*args):
    if args[0][0] is not None:
        return "STACKING_RULE_" + args[0][0]
    return "STACKING_RULE_NULL"


# itemPackageLevels functions - START

def createContainerOrientation(*args):
    if args[0] is None:
        return None
    else:
        outputList = []

        if args[0][0] is not None:
            if args[0][0] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_LWH",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_LWH",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })
        if args[0][1] is not None:
            if args[0][1] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_LHW",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_LHW",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })
        if args[0][2] is not None:
            if args[0][2] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_WLH",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_WLH",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })
        if args[0][3] is not None:
            if args[0][3] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_WHL",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_WHL",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })
        if args[0][4] is not None:
            if args[0][4] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_HLW",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_HLW",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })
        if args[0][5] is not None:
            if args[0][5] == 1:
                outputList.append({
                    "containerOrientationEnumVal": "OR_HWL",
                    "orientationAllowedEnumVal": "ORALW_ALLOWED"
                })
            else:
                outputList.append({
                    "containerOrientationEnumVal": "OR_HWL",
                    "orientationAllowedEnumVal": "ORALW_DISALLOWED"
                })

        if len(outputList) > 0:
            return outputList
        else:
            return None


# itemPackageLevels functions - END

# function is used in tms/item, tms/itemType, wms/itemTranslator
def is_present(*args):
    if args[0][0] in [None, '']:
        return args[0][1]
    return args[0][0]


# function is used in tms/item, tms/itemType, wms/itemTranslator

def check_group_code(*args):
    if args[0][0] is not None:
        return args[0][0]
    return args[0][1]


# function is used in ..?

def combine_desc(*args):
    if len(*args) < 3:
        return None
    return args[0] + args[1] + args[2]


# below prefix functions are used in tms/customer

def prefix_tm_tariff_selection_control_code(*args):
    if args[0] in [None, '']:
        return 'TS_BEST'
    return 'TS_' + args[0]


def prefix_tm_rate_shop_tariff_selection_control_code(*args):
    if args[0] in [None, '']:
        return 'ALL_AR_TARIFFS'
    return 'TCRS_' + args[0]


def prefix_full_address_check_enum(*args):
    if args[0] in [None, '']:
        return 'AC_NULL'
    return 'AC_' + args[0]


def prefix_default_print_mode_selection_enum(*args):
    if args[0] in [None, '']:
        return 'DFT_PRT_SEL_NULL'
    return 'DFT_PRT_SEL_' + args[0]


def prefix_division_selection_enum(*args):
    if args[0] in [None, '']:
        return 'DS_NULL'
    return 'DS_DIV_' + args[0]


def prefix_logistics_group_selection_enum(*args):
    if args[0] in [None, '']:
        return 'LGS_'
    return 'LGS_LGSTGRP_' + args[0]


def prefix_freight_audit_authorization_enum(*args):
    if args[0] in [None, '']:
        return 'FAA_NULL'
    return 'FAA_' + args[0]


def prefix_monetary_information_control_enum(*args):
    if args[0] in [None, '']:
        return 'AUTHORITY_NULL'
    return 'AUTHORITY_' + args[0]


def prefix_user_status_enum(*args):
    if args[0] in [None, '']:
        return 'USER_S_NULL'
    return 'USER_S_' + args[0]


def prefix_glvoucher_reversal_enum(*args):
    if args[0] in [None, '']:
        return 'GLVR_NULL'
    return 'GLVR_' + args[0]


def prefix_user_alternate_properties_access_enum(*args):
    if args[0] in [None, '']:
        return 'USER_ALT_PROP_ACCESS_NULL'
    return 'USER_ALT_PROP_ACCESS_' + args[0]


def prefix_elevation_uom_enum(*args):
    if args[0] in [None, '']:
        return 'UME_NULL'
    return 'UME_' + args[0]


def prefix_temperature_uom_enum(*args):
    if args[0] in [None, '']:
        return 'UMT_NULL'
    return 'UMT_' + args[0]


# TEST OPERATIONS
def get_Dynamic_Seg_Key(*args):
    return 'Dyanamic_Target_Key'


def get_Op_Value(variable):
    return "OP_" + str(variable)


def get_Op_Value_In_Series(variable):
    return "OP_" + str(variable.values[0])


# Wms config changes

def append_str(*args):
    return ('LOC.' + args[0][0]) if args[0][0] is not None else None


def check_boolean(*args):
    if args[0][0] == "1":
        return True
    elif args[0][0] == "0":
        return False
    return None
    # return True if int(args[0][0]) == 1 else False if int(args[0][0]) == 0 else None


def convert_string_to_date(*args):
    from datetime import datetime
    if args[0][0] is None:
        return None
    string = str(args[0][0])
    filtered_string = string[0:8]
    date_object = datetime.strptime(filtered_string, '%Y%m%d')
    date_format = date_object.strftime('%Y-%m-%d')  # format: yyyy-MM-dd
    return date_format


def to_number(value):
    if value[0] is None:
        return None
    if '.' in value[0]:
        return float(value[0])
    return int(value[0])


def negate(value):
    return -(int(value[0])) if value[0] is not None else -1


def generate_uuid_BYDM(*args, **kwargs):
    from uuid import uuid4
    return ('BYDM' + str(uuid4()))


def language_code(*args):
    lower = list(map(lambda x: x.lower(), args[0]))
    return lower[0]


def primary_flag_check(*args):
    if args[0][0] == '1':
        return True
    elif args[0][0] == '0':
        return False
    return args[0][0]


def not_blank_check(*args):
    if args[0][0] is None and args[0][1] is None and args[0][2] is None:
        return False
    return True


# orders - bydm
def check_action_code_state(*args):
    if args[0].upper() == 'CANCELLED':
        return 'CANCEL'


def check_document_action_code(*args):
    if args[0] is None or args[0] == '':
        return 'ADD'
    elif args[0] is not None:
        return 'PARTIAL_CHANGE'
    else:
        check_action_code_state(args[1])


def check_original_order_type_code(*args):
    if args[0][0] == 'SalesOrder':
        return 'CUSTOMER_ORDER'
    elif args[0][0] == 'TransferOrder':
        return 'TRANSFER_ORDER'
    elif args[0][0] == 'PurchaseOrder':
        return 'PURCHASE_ORDER'
    elif args[0][0] == 'ReturnOrder':
        return 'RETURN_ORDER'
    return None


def check_action_code(*args):
    if args[0] is None or args[0] == '':
        return 'ADD'
    elif args[0] is not None:
        return 'CHANGE'
    else:
        check_action_code_state(args[1])


def check_primary_id(*args):
    if args[0][0] is not None:
        return args[0][0]
    return args[0][1]


def prefix_container_type(*args):
    if args[0][0] is None or args[0][0] == '':
        return "CNTRTYPE_CUBE"
    return "CNTRTYPE_" + args[0][0]


def get_one_or_zero(value):
    if value[0] is not None:
        if value[0] is True:
            return 1
        elif value[0] is False:
            return 0
    return None


def get_document_code_wms(value):
    if value[0] == "ADD":
        return 'A'
    elif value[0] == 'DELETE':
        return 'D'
    elif value[0] == 'CHANGE_BY_REFRESH':
        return 'R'
    else:
        return None


def get_document_wms(*arg):
    trnType = {
        "ADD": 'A',
        "DELETE": 'D',
        "CHANGE_BY_REFRESH": 'C'
    }
    return trnType.get(*arg)


def get_transaction_wms(*arg):
    trnType = {
        "Apply": 'A',
        "Release": 'R'
    }
    return trnType.get(*arg)


def get_modified_date1(dateTimeValue):
    import datetime
    from datetime import datetime
    if dateTimeValue[0] is None:
        return None
    datetime_object = datetime.strptime(dateTimeValue[0], "%Y-%m-%d")
    date = datetime.strftime(datetime_object, "%Y%m%d")
    return date


def get_modified_date2(dateTimeValue):
    import datetime
    from datetime import datetime
    if dateTimeValue[0] is None:
        return None
    datetime_object = datetime.strptime(str(dateTimeValue[0]), "%Y-%m-%d %H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y%m%d%H%M%S")
    return date


def calculate_date_from_epoc2(epoc_time):
    if epoc_time[0] is None:
        return None
    else:
        date = epoc_time[0].replace('-', '')
        return (date + '000000')


def calculate_date_time_from_epoc1(epoc_time):
    if (epoc_time[0] is not None):
        import time
        epoc_time = int(epoc_time[0] / 1000)
        date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(epoc_time))
        return date_time
    else:
        return None


# Transport Load Custom Function - START

def get_TL_reciever(args):
    result = []
    stops = args[0]
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, "countOfShipmentsPickedAtStop") is not None:
                if getattr(stop, "countOfShipmentsPickedAtStop") > 0:
                    StopStatusEnumVal = getattr(stop, 'stopStatusEnumVal')
                    not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                    if StopStatusEnumVal not in not_in_list:
                        if hasattr(stop, "shippingLocationCode"):
                            shippingLocationCode = "LOC." + getattr(stop, "shippingLocationCode")
                            result.append(shippingLocationCode)
    else:
        if hasattr(stops, "countOfShipmentsPickedAtStop") is not None:
            if getattr(stops, "countOfShipmentsPickedAtStop") > 0:
                StopStatusEnumVal = getattr(stops, 'stopStatusEnumVal')
                not_in_list = ['SS_PICKEDUP', 'SS_DLVD_PICKEDUP', 'SS_DELIVERED', 'SS_DROP_PNDG']
                if StopStatusEnumVal not in not_in_list:
                    if hasattr(stops, "shippingLocationCode"):
                        shippingLocationCode = "LOC." + getattr(stops, "shippingLocationCode")
                        result.append(shippingLocationCode)

    if len(result) == 1:
        return result[0]

    else:
        return result


def get_TL_messageVersion(args):
    messageVersion = args[0]
    return messageVersion


def getDateComponentForTL_logisticEvent_From(args):
    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_From(args):
    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedArrivalDateTime = args[0]

    if appointment is not None and hasattr(appointment, "appointmentFromDateTime"):
        dateTimeValue = getattr(appointment, "appointmentFromDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time


def getDateComponentForTL_logisticEvent_To(args):
    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedDepartureDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedDepartureDateTime
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def getTimeComponentForTL_logisticEvent_To(args):
    import datetime
    from datetime import datetime
    appointment = args[1]
    lastComputedArrivalDateTime = args[0]
    if appointment is not None and hasattr(appointment, "appointmentToDateTime"):
        dateTimeValue = getattr(appointment, "appointmentToDateTime")
    else:
        dateTimeValue = lastComputedArrivalDateTime
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time


def is_multi_leg_shipment(args):
    numberOfShipmentLeg = args[0]
    if numberOfShipmentLeg > 0:
        return True
    else:
        return False


def is_cross_dock_leg(args):
    numberOfShipmentLeg = args[0]
    SL_shipFromLocationCode = args[1]
    shipFromLocationCode = args[2]

    if numberOfShipmentLeg > 0 and (SL_shipFromLocationCode != shipFromLocationCode):
        return True
    else:
        return False


def get_date_component(args):
    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    date = datetime.strftime(datetime_object, "%Y-%m-%d")
    return date


def get_time_component(args):
    import datetime
    from datetime import datetime
    dateTimeValue = args[0]
    datetime_object = datetime.strptime(dateTimeValue, "%Y-%m-%dT%H:%M:%S")
    time = datetime.strftime(datetime_object, "%H:%M:%S")
    return time


def get_referenceNumberStructure_by_type(args):
    referenceNumberStructures = args[0]
    typecode = args[1]
    if isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_item_id_for_transactionalTradeItem(args):
    TransportOrderInfo_ProductNumberCode = args[0]
    ShipmentItem_ItemNumber = args[1]
    ItemNumber = args[2]
    referenceNumberStructures = args[3]
    typecode = args[4]
    if TransportOrderInfo_ProductNumberCode is not None:
        return TransportOrderInfo_ProductNumberCode
    elif ShipmentItem_ItemNumber is not None:
        return ShipmentItem_ItemNumber
    elif ItemNumber is not None:
        return ItemNumber
    elif isinstance(referenceNumberStructures, list):
        for rf in referenceNumberStructures:
            if hasattr(rf, 'referenceNumberTypeCode'):
                if getattr(rf, 'referenceNumberTypeCode') == typecode:
                    return getattr(rf, 'referenceNumber')
    else:
        if hasattr(referenceNumberStructures, 'referenceNumberTypeCode'):
            if getattr(referenceNumberStructures, 'referenceNumberTypeCode') == typecode:
                return getattr(referenceNumberStructures, 'referenceNumber')

    return None


def get_pickupShipmentReference_for_stops_TL(args):
    pickShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if pickShipmentLegID is not None:
        if isinstance(pickShipmentLegID, list):
            for id in pickShipmentLegID:
                for S_id in ShipmentLegIDs:
                    ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                    if id == ShipmentLegID:
                        output = {}
                        output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                        output['additionalShipmentId'] = [
                            {
                                "typeCode": "GSIN",
                                "value": "00000000000000000"
                            }
                        ]
                        output_list.append(output)
                        break
        return output_list
    else:
        return None


def get_dropoffShipmentReference_for_stops_TL(args):
    dropoffShipmentLegID = args[0]
    ShipmentLegIDs = args[1]
    output_list = []
    if dropoffShipmentLegID is not None:
        if isinstance(dropoffShipmentLegID, list):
            for id in dropoffShipmentLegID:
                for S_id in ShipmentLegIDs:
                    ShipmentLegID = getattr(S_id, 'systemShipmentLegID')
                    if id == ShipmentLegID:
                        output = {}
                        output['primaryId'] = getattr(getattr(S_id, 'shipment'), 'shipmentNumber')
                        output['additionalShipmentId'] = [
                            {
                                "typeCode": "GSIN",
                                "value": "00000000000000000"
                            }
                        ]
                        output_list.append(output)
                        break
        return output_list
    else:
        return None


def get_entityId_transportReference(args):
    loadDescription = args[0]
    referenceNumberStructure = args[1]
    if loadDescription is not None:
        return loadDescription
    elif isinstance(referenceNumberStructure, list):
        for ref_num_stuc in referenceNumberStructure:
            if hasattr(ref_num_stuc, 'referenceNumberTypeCode'):
                if getattr(ref_num_stuc, 'referenceNumberTypeCode') == 'PRO_ID':
                    return getattr(ref_num_stuc, 'referenceNumber')
    elif hasattr(referenceNumberStructure, 'referenceNumberTypeCode'):
        if getattr(referenceNumberStructure, 'referenceNumberTypeCode') == 'PRO_ID':
            return getattr(referenceNumberStructure, 'referenceNumber')

    return None


def get_total_pickup_stops(args):
    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsPickedAtStop'):
                if getattr(stop, 'countOfShipmentsPickedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsPickedAtStop'):
            if getattr(stops, 'countOfShipmentsPickedAtStop') > 0:
                counter += 1
    return counter


def get_total_dropoff_stops(args):
    stops = args[0]

    if stops is None:
        return 0

    counter = 0
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'countOfShipmentsDroppedAtStop'):
                if getattr(stop, 'countOfShipmentsDroppedAtStop') > 0:
                    counter += 1
    else:
        if hasattr(stops, 'countOfShipmentsDroppedAtStop'):
            if getattr(stops, 'countOfShipmentsDroppedAtStop') > 0:
                counter += 1
    return counter


def get_arg_as_list(args):
    output = []
    for arg in args:
        if arg is not None:
            output.append(arg)

    if len(output) == 0:
        return None
    return output


def get_dock_slot_identifier(args):
    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockSlotIdentifier', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockSlotIdentifier', None)


def get_dockId(args):
    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'dockCode', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'dockCode', None)


def get_dock_type_code(args):
    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return None

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                return getattr(dock, 'DockTypeEnumVal', None)

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            return getattr(docks, 'DockTypeEnumVal', None)


def get_systemDockCommitmentID(args):
    shippingLocationCode = args[0]
    docks = args[1]

    if docks is None:
        return False

    elif isinstance(docks, list):
        for dock in docks:
            if getattr(dock, 'shippingLocationCode', None) == shippingLocationCode:
                if getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                    return getattr(getattr(dock, 'dockCommitment')[0], 'systemDockCommitmentID')

    else:
        if getattr(docks, 'shippingLocationCode', None) == shippingLocationCode:
            if getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID') is not None:
                return getattr(getattr(docks, 'dockCommitment')[0], 'systemDockCommitmentID')

    return False


def get_document_action_code_transport_load(args):
    event_name = args[0]
    if event_name in ["LoadTenderUpdated", "ShipmentLegRemovedFromLoad", "ShipmentLegAddedToPlannedLoad"]:
        return "CHANGE_BY_REFRESH"
    if event_name == "LoadTenderAccepted":
        return "ADD"
    if event_name == "LoadTenderCancelled":
        return "DELETE"

    return "CHANGE_BY_REFRESH"


# Transport Load Custom Function - END

# TransporLoad Reference Number Update - START


def get_reference_number_for_load_tl(args):
    stops = args[0]
    eventName = args[1]
    referenceNumber = []
    if eventName in ['LoadTenderCancelled']:
        return None
    if isinstance(stops, list):
        for stop in stops:
            if hasattr(stop, 'shippingLocationCode'):
                referenceNumber.append(f"{str(getattr(stop, 'shippingLocationCode'))}-AWARE")

    else:
        if hasattr(stops, 'shippingLocationCode'):
            referenceNumber.append(f"{str(getattr(stops, 'shippingLocationCode'))}-AWARE")

    return ','.join(referenceNumber)


def get_reference_number_action_enum_for_load_tl(args):
    eventName = args[0]
    if eventName == "LoadTenderAccepted":
        return "AT_ADD"
    elif eventName == "LoadTenderUpdated":
        return "AT_UPDATE"
    elif eventName == "LoadTenderCancelled":
        return "AT_DELETE"
    return None


def get_system_reference_id_for_load_tl(args):
    loadReferenceNumberStructures = args[0]
    eventName = args[1]
    loadWHStatusRefNumber = None
    if isinstance(loadReferenceNumberStructures, list):
        for RNS in loadReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_WH_STS_":
                loadWHStatusRefNumber = getattr(RNS, "SystemReferenceNumberID")

    if loadWHStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled"]:
        return loadWHStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_system_reference_id_stop_load_tl(args):
    ReferenceNumberStructures = args[0]
    eventName = args[1]
    stopStatusRefNumber = None
    if isinstance(ReferenceNumberStructures, list):
        for RNS in ReferenceNumberStructures:
            if getattr(RNS, "referenceNumberTypeCode") == "WM_SHP_STS":
                stopStatusRefNumber = getattr(RNS, "SystemReferenceNumberID")

    if stopStatusRefNumber is not None and eventName in ["LoadTenderUpdated", "LoadTenderCancelled"]:
        return stopStatusRefNumber
    elif eventName == "LoadTenderAccepted":
        return None

    return None


def get_reference_number_for_stop_tl(args):
    shippingLocationCode = args[0]
    eventName = args[1]
    if eventName in ['LoadTenderCancelled']:
        return None
    return shippingLocationCode + "-AWARE"


# TransporLoad Reference Number Update - END

# TO CHECK FOR WORKING_CALENDAR
def working_calendar(args_list):
    result = {}
    calendar_id = args_list[0].values[0]
    day = args_list[1].values[0]
    shift_details = args_list[2].values[0]
    if calendar_id in [None, '']:
        return None
    if day in [None, ''] or shift_details in [None, '']:
        return None
    else:
        data_frame = args_list[3][args_list[3]['CAL_ID'] == args_list[2].values[0]]
        data_frame_regular_weekshift = data_frame[data_frame['W_CALENDARTYPE'] == 'RegularWeekShiftDetails']
        data_frame_override = data_frame[data_frame['W_CALENDARTYPE'] == 'OverrideDays']
        dict = {}
        for i in data_frame_regular_weekshift.index:
            start_time = str(data_frame_regular_weekshift['W_StartTime'][i])
            end_time = str(data_frame_regular_weekshift['W_EndTime'][i])
            day = data_frame_regular_weekshift['W_DAYOFWEEK'][i]
            dict.update({day: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'regularWeekShiftDetails': dict}
        dict = {}
        for i in data_frame_override.index:
            start_time = data_frame_override['W_StartTime'][i]
            end_time = data_frame_override['W_EndTime'][i]
            date = "%s-%s-%s" % (
                data_frame_override['W_YEAR'][i], data_frame_override['W_MONTH'][i], data_frame_override['W_DAY'][i])
            print(start_time, end_time, day)
            dict.update({date: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'overrideDaysDetails': dict}
    return result


# TO CHECK FOR INBOUND_WORKING_CALENDAR
def inbound_working_calendar(args_list):
    result = {}
    calendar_id = args_list[0].values[0]
    enabled = args_list[1].values[0]
    day = args_list[2].values[0]
    shift_details = args_list[3].values[0]

    if day in [None, ''] or shift_details in [None, '']:
        return None
    else:
        data_frame = args_list[3][args_list[3]['IN_CAL_ID'] == args_list[2].values[0]]
        data_frame_regular_weekshift = data_frame[data_frame['IN_CALENDARTYPE'] == 'RegularWeekShiftDetails']
        data_frame_override = data_frame[data_frame['IN_CALENDARTYPE'] == 'OverrideDays']
        dict = {}
        for i in data_frame_regular_weekshift.index:
            start_time = str(data_frame_regular_weekshift['IN_StartTime'][i])
            end_time = str(data_frame_regular_weekshift['IN_EndTime'][i])
            day = data_frame_regular_weekshift['IN_DAYOFWEEK'][i]
            dict.update({day: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'regularWeekShiftDetails': dict}
        dict = {}
        for i in data_frame_override.index:
            start_time = data_frame_override['IN_StartTime'][i]
            end_time = data_frame_override['IN_EndTime'][i]
            date = "%s-%s-%s" % (
                data_frame_override['IN_YEAR'][i], data_frame_override['IN_MONTH'][i], data_frame_override['IN_DAY'][i])
            print(start_time, end_time, day)
            dict.update({date: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'overrideDaysDetails': dict}
    return result


# TO CHECK FOR OUTBOUND_WORKING_CALENDAR
def outbound_working_calendar(args_list):
    result = {}
    day = args_list[0].values[0]
    shift_details = args_list[1].values[0]
    if day in [None, ''] or shift_details in [None, '']:
        return None
    else:
        data_frame = args_list[3][args_list[3]['OUT_CAL_ID'] == args_list[2].values[0]]
        data_frame_regular_weekshift = data_frame[data_frame['OUT_CALENDARTYPE'] == 'RegularWeekShiftDetails']
        data_frame_override = data_frame[data_frame['OUT_CALENDARTYPE'] == 'OverrideDays']
        dict = {}
        for i in data_frame_regular_weekshift.index:
            start_time = str(data_frame_regular_weekshift['OUT_StartTime'][i])
            end_time = str(data_frame_regular_weekshift['OUT_EndTime'][i])
            day = data_frame_regular_weekshift['OUT_DAYOFWEEK'][i]
            dict.update({day: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'regularWeekShiftDetails': dict}
        dict = {}
        for i in data_frame_override.index:
            start_time = data_frame_override['OUT_StartTime'][i]
            end_time = data_frame_override['OUT_EndTime'][i]
            date = "%s-%s-%s" % (
                data_frame_override['OUT_YEAR'][i], data_frame_override['OUT_MONTH'][i],
                data_frame_override['OUT_DAY'][i])
            print(start_time, end_time, day)
            dict.update({date: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'overrideDaysDetails': dict}
    return result


# Custom function for locations-oms config, to check the calendar configuration
def calendar_payload(args_list):
    result = {}
    in_result = {}
    out_result = {}
    final_result = {}

    # workingCalendar
    w_calendar_id = args_list[0].values[0]
    w_day = args_list[1].values[0]
    w_shift_details = args_list[2].values[0]
    # inboundWorkingCalendar
    in_calendar_id = args_list[3].values[0]
    in_day = args_list[4].values[0]
    in_shift_details = args_list[5].values[0]
    # outboundWorkingCalendar
    out_calendar_id = args_list[6].values[0]
    out_day = args_list[7].values[0]
    out_shift_details = args_list[8].values[0]

    # enablement for calendars
    wc_enabled = args_list[9].values[0]
    in_enabled = args_list[10].values[0]
    out_enabled = args_list[11].values[0]

    if w_calendar_id not in [None, ''] and w_day not in [None, ''] and w_shift_details not in [None, '']:
        # WC dataframes

        data_frame = args_list[12][args_list[12]['CAL_ID'] == w_calendar_id]
        data_frame_regular_weekshift = data_frame[data_frame['W_CALENDARTYPE'] == 'working-calendar']
        data_frame_override = data_frame[data_frame['W_CALENDARTYPE'] == 'OverrideDays']
        # WC - structure - regularweekshift
        dict = {}
        for i in data_frame_regular_weekshift.index:
            start_time = str(data_frame_regular_weekshift['W_StartTime'][i])
            end_time = str(data_frame_regular_weekshift['W_EndTime'][i])
            day = data_frame_regular_weekshift['W_DAYOFWEEK'][i]
            dict.update({day: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'calendarId': w_calendar_id, 'enabled': int(wc_enabled) == 1, 'regularWeekShiftDetails': dict}

        # WC - structure - overridedays
        dict = {}
        for i in data_frame_override.index:
            start_time = data_frame_override['W_StartTime'][i]
            end_time = data_frame_override['W_EndTime'][i]
            date = "%s-%s-%s" % (
                data_frame_override['W_YEAR'][i], data_frame_override['W_MONTH'][i], data_frame_override['W_DAY'][i])
            dict.update({date: [{'startTime': start_time, 'endTime': end_time}]})
        if len(dict) > 0:
            result = {'overrideDaysDetails': dict}
        final_result.update({'workingCalendar': result})

    if in_calendar_id not in [None, ''] and in_day not in [None, ''] and in_shift_details not in [None, '']:
        # IWC dataframes
        in_data_frame = args_list[12][args_list[12]['IN_CAL_ID'] == in_calendar_id]
        in_data_frame_regular_weekshift = in_data_frame[in_data_frame['IN_CALENDARTYPE'] == 'inbound-working-calendar']
        in_data_frame_override = in_data_frame[in_data_frame['IN_CALENDARTYPE'] == 'OverrideDays']
        # IWC - structure - regularweekshift
        in_dict = {}
        for i in in_data_frame_regular_weekshift.index:
            in_start_time = str(in_data_frame_regular_weekshift['IN_StartTime'][i])
            in_end_time = str(in_data_frame_regular_weekshift['IN_EndTime'][i])
            in_day = in_data_frame_regular_weekshift['IN_DAYOFWEEK'][i]
            in_dict.update({in_day: [{'startTime': in_start_time, 'endTime': in_end_time}]})
        if len(in_dict) > 0:
            in_result = {'calendarId': in_calendar_id, 'enabled': int(in_enabled) == 1,
                         'regularWeekShiftDetails': in_dict}
        # IWC - structure - overridedays
        in_dict = {}
        for i in in_data_frame_override.index:
            in_start_time = in_data_frame_override['IN_StartTime'][i]
            in_end_time = in_data_frame_override['IN_EndTime'][i]
            in_date = "%s-%s-%s" % (
                in_data_frame_override['IN_YEAR'][i], in_data_frame_override['IN_MONTH'][i],
                in_data_frame_override['IN_DAY'][i])
            in_dict.update({in_date: [{'startTime': in_start_time, 'endTime': in_end_time}]})
        if len(in_dict) > 0:
            in_result = {'overrideDaysDetails': in_dict}
        final_result.update({'inboundWorkingCalendar': in_result})

    if out_calendar_id not in [None, ''] and out_day not in [None, ''] and out_shift_details not in [None, '']:
        # OWC dataframes
        out_data_frame = args_list[12][args_list[12]['OUT_CAL_ID'] == out_calendar_id]
        out_data_frame_regular_weekshift = out_data_frame[
            out_data_frame['OUT_CALENDARTYPE'] == 'outbound-working-calendar']
        out_data_frame_override = out_data_frame[out_data_frame['OUT_CALENDARTYPE'] == 'OverrideDays']
        # OWC - structure - regularweekshift
        out_dict = {}
        for i in out_data_frame_regular_weekshift.index:
            out_start_time = str(out_data_frame_regular_weekshift['OUT_StartTime'][i])
            out_end_time = str(out_data_frame_regular_weekshift['OUT_EndTime'][i])
            out_day = out_data_frame_regular_weekshift['OUT_DAYOFWEEK'][i]
            out_dict.update({out_day: [{'startTime': out_start_time, 'endTime': out_end_time}]})
        if len(out_dict) > 0:
            out_result = {'calendarId': out_calendar_id, 'enabled': int(out_enabled) == 1,
                          'regularWeekShiftDetails': out_dict}
        # OWC - structure - overridedays
        out_dict = {}
        for i in out_data_frame_override.index:
            out_start_time = out_data_frame_override['OUT_StartTime'][i]
            out_end_time = out_data_frame_override['OUT_EndTime'][i]
            out_date = "%s-%s-%s" % (
                out_data_frame_override['OUT_YEAR'][i], out_data_frame_override['OUT_MONTH'][i],
                out_data_frame_override['OUT_DAY'][i])
            out_dict.update({out_date: [{'startTime': out_start_time, 'endTime': out_end_time}]})
        if len(out_dict) > 0:
            out_result = {'overrideDaysDetails': out_dict}
        final_result.update({'outboundWorkingCalendar': out_result})
    if len(final_result) > 0:
        return final_result
    return None


# function is used in OMS-item
def check_item_status_code(*args):
    if args[0][0] == 'A':
        return True
    else:
        return False


# Logistic calendar for TMS
def logistics_calendar_payload(args_list):
    result = {}
    calendar_id = args_list[0].values[0]
    day = args_list[1].values[0]
    utc_offset = args_list[3].values[0]
    shift_details = args_list[2].values[0]
    if calendar_id not in [None, ''] and day not in [None, ''] and shift_details not in [None, '']:
        data_frame = args_list[4][args_list[4]['CAL_ID'] == calendar_id]
        data_frame_logistic_activity = data_frame[data_frame['CAL_TYPE'] == 'logistic-activity-calendar']
        data_frame_override_days = data_frame[data_frame['CAL_TYPE'] == 'OverrideDays']
        if str(utc_offset) not in [None, '']:
            dict = {'timeZoneOffset': str(utc_offset)}
        else:
            dict = {}
        dict_dow = {"MONDAY": "mon", "TUESDAY": "tue", "WEDNESDAY": "wed", "THURSDAY": "thu", "FRIDAY": "fri",
                    "SATURDAY": "sat", "SUNDAY": "sun"}
        df = data_frame_logistic_activity.groupby('DAYOFWEEK')
        for dow_group in df.groups:
            df_dow_group = df.get_group(dow_group)
            counter = 1
            for i in df_dow_group.index:
                day_of_week = data_frame['DAYOFWEEK'][i]
                if counter > 4:
                    break
                st = str(df_dow_group['CAL_ST'][i])
                et = str(df_dow_group['CAL_ET'][i])
                if st not in [None, ''] and et not in [None, '']:
                    dict.update({f'{dict_dow[day_of_week]}BusinessHours{counter}Open': st,
                                 f'{dict_dow[day_of_week]}BusinessHours{counter}Close': et})
                    counter += 1
        holiday_list = []
        for i in data_frame_override_days.index:
            date = str(data_frame_override_days['YEAR'][i]) + '-' + str(
                data_frame_override_days['MONTH'][i]) + '-' + str(data_frame_override_days['DAY'][i])
            st = str(data_frame_override_days['CAL_ST'][i])
            et = str(data_frame_override_days['CAL_ET'][i])
            holiday_list.append({'holidayDate': date, 'holidayFromTime': st, 'holidayToTime': et})
        if len(holiday_list) > 0:
            dict.update({"holiday": holiday_list})
        if len(dict) > 0:
            result = {'businessHours': dict}
    if len(result) > 0:
        return result
    return None


# function used in loca elastic search
def append_strings(*args):
    if args[0][0] not in [None, ''] and args[0][1] not in [None, '']:
        return args[0][0] + ' ' + args[0][1]
    elif args[0][0] not in [None, '']:
        return args[0][0]
    elif args[0][1] not in [None, '']:
        return args[0][1]
    return None


def check_null(*args):
    if args[0][0] in [None, '']:
        return False
    return True


# TMS function

# Use Case:
#     Get All container from all the shipments
# Used In: Process shipment container maintain API
# Input: shipment Number, original payload, global variables
# output: from all despatch advice record filter out all the shipment lines which is having
#         shipment number given in the argument from level '4' and not having any despatched quantity as 0.

def get_unique_containers(shipment_number, payload, global_variable):
    level_id = list()
    shipment_id = list()
    unique_line = {}
    path = ["despatchAdviceLogisticUnit", "levelId"]
    path1 = ["lineItem", "transactionalReference", "entityId"]
    path2 = ["lineItem", "despatchedQuantity", "value"]
    path3 = ["lineItem", "transactionalReference", "lineItemNumber"]

    def get_data(payload, itr, path, n, ans, levelId, shipment_number, qty, filter_level, current_filter_level,
                 parent_seg, unique_lines):
        if itr == n:
            if isinstance(payload, list):
                data1 = []
                for data in payload:
                    if shipment_number is not None and data == shipment_number[0]:
                        unique_lines[data] = True
                        return parent_seg
                    elif unique_lines is not None and not unique_line.get(data, None):
                        unique_line[data] = True
                        return parent_seg
            else:
                if levelId is not None and payload != levelId:
                    return parent_seg
                elif shipment_number is not None:
                    return parent_seg
                elif unique_lines is not None and not unique_line.get(payload, None):
                    unique_line[payload] = True
                    return parent_seg
                elif qty is not None and payload > 0:
                    return parent_seg
                else:
                    return
        if not isinstance(payload, list):
            items = eval('payload.' + f'{path[itr]}')
            if filter_level > current_filter_level:
                par_data = items
            else:
                par_data = parent_seg
            if isinstance(items, list):
                for data in items:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    output = get_data(data, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                      current_filter_level + 1, par_data, unique_lines)
                    if output is not None:
                        ans.append(output)
            else:
                output = get_data(items, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                  current_filter_level + 1, par_data, unique_lines)
                if output is not None:
                    ans.append(output)
        else:
            for data in payload:

                try:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    items = eval('data.' + f'{path[itr]}')

                    output = get_data(items, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                      current_filter_level + 1, par_data, unique_lines)

                    if output is not None:
                        ans.append(output)
                except:
                    pass

    for da_doc in eval('payload.despatchAdvice'):
        if eval('da_doc.documentActionCode') != 'DELETE':
            get_data(da_doc, 0, path, 2, level_id, 9, None, None, 1, 0, "", None)
    get_data(level_id, 0, path1, 3, shipment_id, None, shipment_number, None, 1, 0, "", None)
    level_id = []
    positive_qty_lines = level_id
    get_data(shipment_id, 0, path2, 3, positive_qty_lines, None, None, 1, 1, 0, "", None)
    shipment_id = []
    unique_line_item = shipment_id
    get_data(positive_qty_lines, 0, path3, 3, unique_line_item, None, None, None, 1, 0, "", "unique_lines")
    return unique_line_item


# Use Case:
#     Get All lines for a given shipment line and it will be used in the qty update
# Used In: Process shipment container maintain API
# Input: current_shipment item, shipmentNumber, adviceOrderNumber, adviceLineItemNumber
# output: Return the aggregated lines of a given shipment and shipment line

def get_aggregated_Lines(argument_list, payload, global_variable):
    level_id = list()
    shipment_id = list()
    path = ["despatchAdvice", "despatchAdviceLogisticUnit", "levelId"]
    path1 = ["lineItem", "transactionalReference", "entityId"]
    path2 = ["lineItem", "despatchedQuantity", "value"]
    path3 = ["lineItem", "transactionalReference"]

    def get_data(payload, itr, path, n, ans, levelId, shipment_number, qty, filter_level, current_filter_level,
                 parent_seg):
        if itr == n:
            if isinstance(payload, list):
                for data in payload:
                    try:
                        if shipment_number is not None and data == shipment_number:
                            return parent_seg
                        elif eval('data.entityId') == argument_list[2] and eval('data.lineItemNumber') == \
                                argument_list[3]:
                            return parent_seg
                    except:
                        pass
            else:
                try:
                    if levelId is not None and payload != levelId:
                        return parent_seg
                    elif shipment_number is not None and payload == shipment_number:
                        return parent_seg
                    elif qty is not None and payload > 0:
                        return parent_seg
                    elif eval('payload.entityId') == argument_list[2] and eval('payload.lineItemNumber') == \
                            argument_list[3]:
                        return parent_seg
                    else:
                        return
                except:
                    return
        if not isinstance(payload, list):

            items = eval('payload.' + f'{path[itr]}')
            if filter_level > current_filter_level:
                par_data = items
            else:
                par_data = parent_seg

            if isinstance(items, list):
                for data in items:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    output = get_data(data, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
            else:
                output = get_data(items, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                  current_filter_level + 1, par_data)
                if output is not None:
                    ans.append(output)
        else:
            for data in payload:
                try:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n, ans, levelId, shipment_number, qty, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    get_data(payload, 0, path, 3, level_id, 9, None, None, 2, 0, "")
    get_data(level_id, 0, path1, 3, shipment_id, None, argument_list[1], None, 1, 0, "")
    level_id = []
    positive_qty_item = level_id
    get_data(shipment_id, 0, path2, 3, positive_qty_item, None, None, 1, 1, 0, "")
    shipment_id = []
    line_items = shipment_id
    get_data(positive_qty_item, 0, path3, 2, line_items, None, None, None, 2, 0, "")
    return {"aggregatedLine": line_items}


# Use Case:
#     Get the quantity sum of a line
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the qty sum

def get_aggregated_qty_sum(argument_list, original_payload, global_variable):
    if len(argument_list) == 0:
        return 0
    qty_sum = 0
    for item in argument_list[0].get("aggregatedLine"):
        qty_sum += eval('item.despatchedQuantity.value')
    return qty_sum


# Use Case:
#     Get the unit volume
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the net volume

def get_aggregated_unit_net_volume(argument_list, payload, global_variable):
    if len(argument_list) == 0:
        return 0
    qty_sum = 0
    for item in argument_list[0].get("aggregatedLine"):
        try:
            qty_sum += eval('item.avpList.value')
        except:
            return 0
    return qty_sum


# Use Case:
#     Get the unit weight
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the net weight
def get_aggregated_unit_weight(argument_list, payload, global_variable):
    if len(argument_list) == 0:
        return 0
    qty_sum = 0
    for item in argument_list[0].get("aggregatedLine"):
        qty_sum += eval(
            'item.transactionalTradeItem.transactionalItemData[0].transactionalItemWeight[0].measurementValue.value')
    return qty_sum


# Use Case:
#     Get the total volumn of a container
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the total volume
def get_aggregated_total_volume(argument_list, payload, global_variable):
    if len(argument_list) == 0:
        return 0
    qty_sum = 0
    for item in argument_list[0].get("aggregatedLine"):
        try:
            qty_sum += eval('item.totalGrossVolume.value')
        except:
            return 0
    return qty_sum


# Use Case:
#     Get the total weight of a container
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the total weight of container
def get_total_weight_of_container(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    calculated_weight = ConversionUtils.getUomConversion("Weight", argument_list[2], argument_list[4],
                                                         argument_list[1])

    if argument_list[0] == 'TOTAL_GROSS_WEIGHT' and argument_list[1] > 0:
        return calculated_weight
    elif argument_list[1] > 0:
        return round((calculated_weight * int(argument_list[3])), 4)
    else:
        return 0


# Use Case:
#     Get the total volume of a container
# Used In: Process shipment container maintain API
# Input: aggregatedLinesDoc
# output: Return the total volume of container
def get_total_volume_of_container(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    calculated_volume = ConversionUtils.getUomConversion("Volume", argument_list[2], argument_list[5], argument_list[1])
    if argument_list[0] != 'unitNetVolume':
        return calculated_volume
    elif argument_list[1] > 0:
        return round((calculated_volume * int(argument_list[3])), 4)
    else:
        return 0


# Use Case:
#     Get the weight
# Used In: Process shipment container maintain API
# Input: inputWeightType, aggregatedInputWeightValue, measurementUnitCode, aggregatedQuantity,totalWeightOfContainer,property: tms.uom.weight
# output: Return the calculated weight
def get_input_weight(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    calculated_weight = ConversionUtils.getUomConversion("Weight", argument_list[2], argument_list[5], argument_list[1])
    if argument_list[0] == 'TOTAL_GROSS_WEIGHT' and argument_list[1] > 0:
        return round((calculated_weight / int(argument_list[3])), 4)
    elif argument_list[4] > 0:
        return round((argument_list[4] / int(argument_list[3])), 4)
    else:
        return 0


# Use Case:
#     Get the container unit volume
# Used In: Process shipment container maintain API
# Input: inputWeightType, aggregatedInputWeightValue, measurementUnitCode, aggregatedQuantity,totalWeightOfContainer,property: tms.uom.weight
# output: Return the calculated volume
def get_container_unit_volume(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    calculated_volume = ConversionUtils.getUomConversion("Volume", argument_list[2], argument_list[5], argument_list[1])
    if argument_list[0] == 'unitNetVolume':
        return round((calculated_volume / int(argument_list[3])), 4)
    elif argument_list[4] > 0:
        return round((argument_list[4] / int(argument_list[3])), 4)
    else:
        return 0


# Use Case:
#     Get the shipment container document of container
# Used In: Process shipment container maintain API
# Input: current_shipment, adviceLineItemNumber
# output: Return the calculated volume

def get_shipment_container_document(argument_list, payload, global_variable):
    path1 = ["container", "ReferenceNumberStructure", "ReferenceNumberTypeCode"]
    path2 = ["container", "systemContainerID"]
    ans = list()

    def get_data(payload, itr, path, n, ans, value, filter_level, current_filter_level,
                 parent_seg):
        if itr == n:
            if isinstance(payload, list):
                for data in payload:
                    if value is not None and data == value:
                        return parent_seg
            else:
                if value is not None and payload == value:
                    return parent_seg
                else:
                    return
        if not isinstance(payload, list):
            try:
                items = eval('payload.' + f'{path[itr]}')
                if filter_level > current_filter_level:
                    par_data = items
                else:
                    par_data = parent_seg
                if isinstance(items, list):
                    for data in items:
                        if filter_level > current_filter_level:
                            par_data = data
                        else:
                            par_data = parent_seg
                        output = get_data(data, itr + 1, path, n, ans, value, filter_level,
                                          current_filter_level + 1, par_data)
                        if output is not None:
                            ans.append(output)
                else:
                    output = get_data(items, itr + 1, path, n, ans, value, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
            except:
                pass
        else:
            for data in payload:
                try:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n, ans, value, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    # get_data(argument_list[0], 0, path1, 3, ans, "WM_ORDLIN_", 3, 0, "")
    ans1 = list()
    get_data(argument_list[0], 0, path2, 2, ans1, str(argument_list[1]), 1, 0, "")
    return ans1[0] if len(ans1) else None


# Use Case:
#     Remove a set of field from the input
# Used In: Process shipment container maintain API
# Input: Namespace data along with the tag which will be removed from the input
# output: Return the flat dictionary of the new data
def remove_field_from_payload(argument_list):
    if argument_list[0] is None:
        return None
    import copy
    data = copy.copy(argument_list[0])
    for tag in argument_list[1:]:
        try:
            delattr(data, tag)
        except:
            pass
    item = data.__dict__
    return item


# Use Case:
#     Remove a set of field from the nested Namespace input
# Used In: Process shipment container maintain API
# Input: Nested Namespace data along with the tag which will be removed from the input
# output: Return the flat dictionary of the new data
def remove_field_from_payload_with_nested_namespace(argument_list):
    import json
    import types
    import copy

    # We still need this encoding/decoding to convert nested SimpleNamespace to dictionary
    # in remove_keys function.
    class NamespaceEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, types.SimpleNamespace):
                return vars(obj)
            return json.JSONEncoder.default(self, obj)

    if argument_list[0] is None:
        return None
    data = copy.deepcopy(argument_list[0])
    for tag in argument_list[1:]:
        try:
            delattr(data, tag)
        except:
            pass
    item = json.loads(json.dumps(data, cls=NamespaceEncoder))
    return item


# Use Case:
#     Based on the shipment mode value return data
# Used In: Process shipment container maintain API
# Input: shipmentContainerDocument, shipmentMode
# output: Return the flat dictionary of the new data
def add_shipment_mode(argument_list):
    if argument_list[0] is None:
        return None
    if argument_list[1] != "ILD_SUMMARY_MANDATORY":
        return {
            "ignoreShipmentItemsFlag": True,
            "ignoreWeightByFreightClassFlag": False
        }
    else:
        return None


def get_unique_shipment(shipment_number, payload, global_variable):
    shipment_id = list()
    unique_shipment = {}
    path = ["despatchAdvice", "despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]

    def get_data(payload, itr, path, n, ans, filter_level, current_filter_level,
                 parent_seg):
        if itr == n:

            if unique_shipment.get(eval('payload.entityId'), None) is not True \
                    and eval('payload.transactionalReferenceTypeCode', None) == "SRN":
                unique_shipment[eval('payload.entityId')] = True
                return payload

        if not isinstance(payload, list):
            try:
                items = eval('payload.' + f'{path[itr]}')
                if filter_level > current_filter_level:
                    par_data = items
                else:
                    par_data = parent_seg

                if isinstance(items, list):
                    for data in items:
                        if filter_level > current_filter_level:
                            par_data = data
                        else:
                            par_data = parent_seg
                        output = get_data(data, itr + 1, path, n, ans, filter_level,
                                          current_filter_level + 1, par_data)
                        if output is not None:
                            ans.append(output)
                else:
                    output = get_data(items, itr + 1, path, n, ans, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
            except:
                pass
        else:
            for data in payload:
                try:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n, ans, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    get_data(payload, 0, path, 4, shipment_id, 4, 0, "")
    return shipment_id


def get_current_load(load_id, payload, global_variable):
    load = eval('payload.load.data')
    if load is None:
        return None
    for load_doc in load:
        if eval('load_doc.systemLoadID') == load_id[0]:
            return load_doc
    return None


def get_shipping_location_information_document(shipping_id, payload, global_variable):
    shipping_location = eval('payload.shippingLocation.data')
    if shipping_location is None:
        return None
    for shipping_doc in shipping_location:
        if eval('shipping_doc.shippingLocationCode') == shipping_id[0]:
            return shipping_doc

    return None


def get_locationOffset():
    pass


def get_input_departure_datetime():
    pass


def get_mbol_value(argument_list, payload, global_variable):
    current_da = argument_list[0]
    try:
        logistic_service_code = eval('current_da.load.logisticServiceRequirementCode')

        if logistic_service_code == "2":
            return eval('current_da.load.billOfLadingNumber.entityId')
        elif logistic_service_code == "3":
            return eval('current_da.despatchAdviceTransportInformation.billOfLadingNumber.entityId')
        else:
            return None
    except:
        return None


def get_tractor_num(argument_list, payload, global_variable):
    current_da = argument_list[0]
    if eval('current_da.documentActionCode') != 'DELETE':
        try:
            return eval('current_da.despatchAdviceTransportInformation.transportMeansID')
        except:
            return None
    return None


def get_trailer_num(argument_list, payload, global_variable):
    current_da = argument_list[0]
    if eval('current_da.documentActionCode') != 'DELETE':
        try:
            return eval('current_da.load.transportEquipment.assetId[0].primaryId')
        except:
            return None
    return None


def form_stop_look_up(argument_list, payload, global_variable):
    shipping_location_system_legId = {}
    stops = eval('payload.load.data[0].stop')
    loads = eval('payload.load.data')
    for load in loads:
        stops = eval('load.stop')
        load_id = eval('load.systemLoadID')
        for stop in stops:
            try:
                ShippingLocationCode = eval('stop.shippingLocationCode')
                StopSequenceNumber = eval('stop.stopSequenceNumber')
                shipping_location_system_legId[f'{ShippingLocationCode}_{load_id}'] = StopSequenceNumber
            except:
                pass
    return shipping_location_system_legId


def get_shipment_leg(argument_list, payload, global_variable):
    shipment_list = eval('payload.shipment.data')
    shipment_leg = []
    for shipment in shipment_list:
        shipment_leg.append(*eval('shipment.shipmentLeg'))
    return shipment_leg


def get_data_from_lookup(argument_list, payload=None, global_variable=None):
    return argument_list[2].get(f'{argument_list[0]}_{argument_list[1]}', None)


def get_mbol_number(argument_list):
    mbol = argument_list[0]
    load_doc = argument_list[1]
    try:
        for reference_number in eval('load_doc.referenceNumberStructure'):
            if eval('reference_number.referenceNumberTypeCode') == 'MB' and eval(
                    'reference_number.ReferenceNumber') is not None:
                return mbol
    except:
        pass
    return None


def get_load_ref(argument_list, payload, global_variable):
    load_doc = argument_list[0]
    for reference_number in eval('load_doc.referenceNumberStructure'):
        if eval('reference_number.referenceNumberTypeCode') == 'PRO_ID' and eval(
                'reference_number.ReferenceNumber') is not None:
            return reference_number
    return None


# def get_load_description(argument_list,payload, global_variable):
#     try:
#         return eval('argument_list[0].loadDescription')
#     except:
#         return None

def get_load_description(argument_list):
    load_doc = argument_list[0]
    prod_id = False
    for reference_number in eval('load_doc.referenceNumberStructure'):
        if eval('reference_number.referenceNumberTypeCode') == 'PRO_ID' and eval(
                'reference_number.referenceNumber') is not None:
            prod_id = True
            break
    if prod_id:
        try:
            load_desc = eval('argument_list[0].loadDescription')
            if load_doc:
                return eval('argument_list[1].load.proNumber.entityId')
        except:
            return None
    return None


def get_trailer_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].trailerLicenseNumber')
        except:
            return None
    return None


def get_tractor_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].truckLicenseNumber')
        except:
            return None
    return None


def get_driver_license_number(argument_list):
    if eval('argument_list[0].documentActionCode') != 'DELETE':
        try:
            return eval('argument_list[0].driverLicenseNumber')
        except:
            return None
    return None


def get_despatch_advice_with_srn(argument_list, payload, global_variable):
    tm_despatch = []
    tm_load = []
    path = ["despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]

    def get_data(payload, itr, path, n):
        if itr == n:
            if eval('payload.transactionalReferenceTypeCode') == "SRN" and eval('payload.entityId') is not None:
                return True
        if not isinstance(payload, list):
            try:
                items = eval('payload.' + f'{path[itr]}')
                if isinstance(items, list):
                    for index, data in enumerate(items):
                        output = get_data(data, itr + 1, path, n)
                        if output is not None:
                            return output
                else:
                    output = get_data(items, itr + 1, path, n)
                    return output
            except:
                pass
        else:
            for index, data in enumerate(payload):
                try:
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n)
                    if output is not None:
                        return output
                except:
                    pass

    for item in eval('payload.despatchAdvice'):
        if eval('item.documentActionCode') != 'DELETE':
            output = get_data(item, 0, path, 3)
            if output is not None:
                tm_despatch.append(item)
    for item in tm_despatch:
        if eval('item.despatchAdviceTransportInformation.transportLoadId') is not None:
            tm_load.append(item)

    return tm_load


def get_unique_load_despatch_advice(argument_list, payload, global_variable):
    unique_load_id = {}
    da_document = []
    for item in eval('payload.despatchAdvice'):
        if eval('item.documentActionCode') != 'DELETE' and eval(
                'item.despatchAdviceTransportInformation.transportLoadId') is not None \
                and unique_load_id.get(eval('item.despatchAdviceTransportInformation.transportLoadId'), None) is None:
            da_document.append(item)

    return da_document if len(da_document) else None


def get_load_reference_id(argument_list, payload, global_variable):
    try:
        for refrence_number_document in eval('argument_list[0].referenceNumberStructure'):
            if eval('refrence_number_document.referenceNumberTypeCode') == 'WM_WH_STS_':
                return eval('refrence_number_document.systemReferenceNumberID')
    except:
        return None


def get_stop_status(argument_list, payload, global_variable):
    return f'{argument_list[0]}{argument_list[1]}'


def get_stop_document(argument_list, payload, global_variable):
    try:
        for stop_doc in eval('argument_list[0].stop'):
            if eval('stop_doc.shippingLocationCode') == argument_list[1]:
                return stop_doc
    except:
        return None


def get_load_status(argument_list, payload, global_variable):
    ans = ''

    def get_data(stop_doc):
        for ref_structure in eval('stop_doc.referenceNumberStructure'):
            if eval('ref_structure.referenceNumberTypeCode') == 'WM_SHP_STS':
                return eval('ref_structure.referenceNumber')

    try:
        for index, stop_doc in enumerate(eval('argument_list[0].stop')):
            try:
                if eval('stop_doc.referenceNumberStructure') is not None:
                    if index == 0 and eval('stop_doc.shippingLocationCode') == argument_list[1]:
                        ans += f',{argument_list[2]}'
                    elif index == 0:
                        ans += get_data(stop_doc)
                    elif eval('stop_doc.shippingLocationCode') == argument_list[1]:
                        ans += f',{argument_list[2]}'
                    else:
                        ans += f',{get_data(stop_doc)}'
            except:
                pass
    except:
        return None
    return ans if len(ans) < 30 else ans[0:30]


def get_stop_reference_number(argument_list, payload, global_variable):
    try:
        for stop_doc in eval('argument_list[0].referenceNumberStructure'):

            if eval('stop_doc.referenceNumberTypeCode') == 'WM_SHP_STS':
                return eval('stop_doc.systemReferenceNumberID')
    except:
        return None
    return None


def get_tm_property(argument_list, payload, global_variable):
    return argument_list[0]


def get_reference_number_action_enum_val(argument_list):
    referenceNumberActionEnumVal = argument_list[0]
    ref_id = argument_list[1]
    if ref_id in [None, '']:
        return {'referenceNumberActionEnumVal': 'AT_ADD'}
    else:
        return {'referenceNumberActionEnumVal': referenceNumberActionEnumVal,
                "systemReferenceNumberID": ref_id}


def get_containers_for_delete(argument_list, payload, global_variable):
    from types import SimpleNamespace as Namespace
    da_shipments = argument_list[0]
    try:
        shipment_containers = eval('payload.shipment.data')
        shipment_shipment_line_combination = {}
        deleted_container = []
        for item in da_shipments:
            shipment_shipment_line_combination[f'{eval("item.entityId")}_{eval("item.lineItemNumber")}'] = True
        for shipment in shipment_containers:
            for curr_container in eval('shipment.container'):
                container_id = eval('curr_container.systemContainerID')
                shipment_number = eval('shipment.shipmentNumber')
                if shipment_shipment_line_combination.get(f'{shipment_number}_{container_id}', None) is None:
                    dict = {"deletedSystemContainerId": container_id}
                    deleted_container.append(Namespace(**dict))

        return deleted_container
    except:
        return None


def form_look_for_shipment(argument_list, payload, global_variable):
    try:
        shipment_containers = eval('payload.shipment.data')
        shipment = {}
        for item in shipment_containers:
            shipment[f'{eval("shipment.shipmentNumber")}'] = item
        return shipment
    except:
        return None


def get_container_action_val(argument_list):
    return "AT_UPDATE" if len(argument_list[0]) else None


def get_missing_shipment(argument_list, payload, global_variable):
    stop_doc = argument_list[0]
    shipment_leg = argument_list[1]
    pick_up_shipments = set()
    try:
        for pick_up_shipment in eval('stop_doc.pickShipmentLegID'):
            pick_up_shipments.add(pick_up_shipment)
    except:
        pass
    for shipment_in_shipment_leg in shipment_leg:
        try:
            pick_up_shipments.remove(eval('shipment_in_shipment_leg.systemShipmentLegID'))
        except:
            # When shipment doesn't exist in the pick up shipment ref,
            # Assign shipment from one load to another
            pass
    return list(pick_up_shipments) if len(pick_up_shipments) else None


def get_missing_shipments_for_ref_update(argument_list, payload, global_variable):
    stop_doc = argument_list[0]
    shipment_leg = argument_list[1]
    pick_up_shipments = set()
    for pick_up_shipment in eval('stop_doc.pickShipmentLegID'):
        pick_up_shipments.add(pick_up_shipment)
    for shipment_in_shipment_leg in shipment_leg:
        try:
            pick_up_shipments.remove(eval('shipment_in_shipment_leg.systemShipmentLegID'))
        except:
            pass
    from types import SimpleNamespace as Namespace
    return list(map(lambda x: Namespace(**{"shipment_id": x}), pick_up_shipments)) if len(pick_up_shipments) else None


def get_reference_number_entity_key(argument_list):
    load_document = argument_list[0]
    removed_shipment = argument_list[1]
    for shipment_leg in eval('load_document.shipmentLeg'):
        try:
            if eval('shipment_leg.systemShipmentLegID') == removed_shipment:
                return eval('shipment_leg.shipment.systemShipmentID')
        except:
            pass
    return None


def get_shipment_look_table(argument_list, payload, global_variable):
    shipment_ids = {}
    try:
        for shipment in eval('payload.shipment.data'):
            shipment_ids[eval('shipment.shipmentNumber')] = shipment
    except:
        pass
    return shipment_ids


def get_unique_shipment_lookup_from_payload(shipment_number, payload, global_variable):
    shipment_id = list()
    unique_shipment = {}
    path = ["despatchAdvice", "despatchAdviceLogisticUnit", "lineItem", "transactionalReference"]

    def get_data(payload, itr, path, n, ans, filter_level, current_filter_level,
                 parent_seg):
        if itr == n:

            if unique_shipment.get(eval('payload.entityId'), None) is not True \
                    and eval('payload.transactionalReferenceTypeCode', None) == "SRN":
                unique_shipment[f"{eval('payload.entityId')}_{eval('parent_seg.shipFrom.primaryId')}"] = parent_seg
                return payload

        if not isinstance(payload, list):
            try:
                items = eval('payload.' + f'{path[itr]}')
                if filter_level > current_filter_level:
                    par_data = items
                else:
                    par_data = parent_seg

                if isinstance(items, list):
                    for data in items:
                        if filter_level > current_filter_level:
                            par_data = data
                        else:
                            par_data = parent_seg
                        output = get_data(data, itr + 1, path, n, ans, filter_level,
                                          current_filter_level + 1, par_data)
                        if output is not None:
                            ans.append(output)
                else:
                    output = get_data(items, itr + 1, path, n, ans, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
            except:
                pass
        else:
            for data in payload:
                try:
                    if filter_level > current_filter_level:
                        par_data = data
                    else:
                        par_data = parent_seg
                    items = eval('data.' + f'{path[itr]}')
                    output = get_data(items, itr + 1, path, n, ans, filter_level,
                                      current_filter_level + 1, par_data)
                    if output is not None:
                        ans.append(output)
                except:
                    pass

    get_data(payload, 0, path, 4, shipment_id, 1, 0, "")
    return unique_shipment


def get_shipment_as_list(shipment_number, payload, global_variable):
    return [shipment_number[0]]


def get_reference_number_update_data(shipment_number):
    if shipment_number is not None:
        return {
            "referenceNumberTypeCode": "WM_REPLAN_YN",
            "referenceNumberCategoryEnumVal": "RNC_SHPM",
            "referenceNumber": "DO_NOT_REPLAN",
            "referenceNumberActionEnumVal": "AT_ADD"
        }


# WON Functions

def map_shipment_to_won_containers(shipment, payload, global_vars):
    from types import SimpleNamespace

    # helper functions
    def flatten(iterables):
        import itertools
        return itertools.chain.from_iterable(iterables)

    def has_SRN_transactional_ref(shipmentItem):
        transactionReference = getattr(shipmentItem, "transactionalReference", None)
        transactionalReferenceTypeCode = filter(lambda x: getattr(x, "transactionalReferenceTypeCode", None) == 'SRN',
                                                transactionReference)
        return len(list(transactionalReferenceTypeCode)) > 0

    def filtered_payload_shipment_items(won_objs):
        if not isinstance(won_objs, list):
            won_objs = [won_objs]
        shipments = []
        for won in won_objs:
            if hasattr(won, "shipment"):
                shipment_seg = getattr(won, "shipment")
                if isinstance(shipment_seg, list):
                    shipments.extend(shipment_seg)
                else:
                    shipments.append(shipment_seg)

        print("filtered_payload_shipment_items -> shipments")
        print(shipments)

        shipmentItems = []
        for shipment in shipments:
            if getattr(shipment, "warehousingOutboundStatusCode", None) != 'STOCK_ALLOCATED' and hasattr(shipment,
                                                                                                         "shipmentItem"):
                shipmentItem_seg = getattr(shipment, "shipmentItem")
                if isinstance(shipmentItem_seg, list):
                    shipmentItems.extend(shipmentItem_seg)
                else:
                    shipmentItems.append(shipmentItem_seg)

        print("filtered_payload_shipment_items -> shipmentItems")
        print(shipmentItems)

        filteredShipmentItems = []
        for item in shipmentItems:
            if getattr(getattr(item, "plannedDespatchQuantity", None), "value",
                       None) != '0' and has_SRN_transactional_ref(item):
                filteredShipmentItems.append(item)

        print("filtered_payload_shipment_items -> filteredShipmentItems")
        print(filteredShipmentItems)
        return filteredShipmentItems
        # shipment[].warehousingOutboundStatusCode != "STOCK_ALLOCATED"
        # shipment[].shipmentItem[].plannedDespatchQuantity.value != "0"
        # shipment[].shipmentItem[].transactionalReference[].transactionalReferenceTypeCode == 'SRN'
        # won_objs = filter(lambda x: hasattr(x, "shipment"), won_objs)
        # shipments = flatten(map(lambda x: x.shipment, won_objs))
        #
        # shipments = filter(lambda x: getattr(x, "warehousingOutboundStatusCode", None) != 'STOCK_ALLOCATED' and hasattr(x, "shipmentItem"), shipments)
        # shipmentItem = flatten(map(lambda x: x.shipmentItem, shipments))
        # shipmentItem = filter(lambda x: getattr(getattr(x, "plannedDespatchQuantity", None), "value", None) != '0', shipmentItem)
        # shipmentItem = filter(has_SRN_transactional_ref, shipmentItem)
        # return list(shipmentItem)
        #

    def calculate_volume(payload_shipment_item):
        return round(payload_shipment_item.logisticUnit[0].unitMeasurement[
                         0].measurementValue.value / payload_shipment_item.plannedDespatchQuantity.value, 4)

    def calculate_weight(payload_shipment_item):
        return payload_shipment_item.logisticUnit[
            0].grossWeight.value / payload_shipment_item.plannedDespatchQuantity.value

    def has_WM_ORDLIN_container(shipment):
        containers = getattr(shipment, "container", None)
        if containers is None:
            return False
        referenceNumberStructure = []
        if not isinstance(containers, list):
            containers = [containers]
        for container in containers:
            ref_num_struct = getattr(container, "referenceNumberStructure", None)
            if isinstance(ref_num_struct, list):
                referenceNumberStructure.extend(ref_num_struct)
            else:
                referenceNumberStructure.append(ref_num_struct)

        typecodes = []
        for rns in referenceNumberStructure:
            type_code = getattr(rns, "referenceNumberTypeCode", None)
            if type_code is not None:
                typecodes.append(type_code)

        for t in typecodes:
            if "WM_ORDLIN_" in t:
                return True
        return False
        # referenceNumberStructure = flatten(map(lambda x: x.referenceNumberStructure, containers))
        # type_codes = map(lambda x: x.referenceNumberTypeCode, referenceNumberStructure)
        # type_codes = filter(lambda e: "WM_ORDLIN_" in e, type_codes)
        # return next(type_codes) is not None

    def is_WM_ORDLIN_container(container):
        referenceNumberStructure = container.referenceNumberStructure
        type_codes = map(lambda x: x.referenceNumberTypeCode, referenceNumberStructure)
        type_codes = filter(lambda e: "WM_ORDLIN_" in e, type_codes)
        return next(type_codes) is not None

    def filtered_enriched_shipment(shipments):
        # shipment[].currentShipmentOperationalStatusEnumVal != "S_CONFIRMED"
        # shipment[].container[].referenceNumberStructure[].referenceNumberTypeCode contains "WM_ORDLIN_"
        # print(shipments)
        # shipments = filter(lambda x: x.currentShipmentOperationalStatusEnumVal != "S_CONFIRMED", shipments)
        # print(list(shipments))
        # shipments = filter(has_WM_ORDLIN_container, shipments)

        if not isinstance(shipments, list):
            shipments = [shipments]

        filteredShipments = []
        for shipment in shipments:
            if getattr(shipment, "currentShipmentOperationalStatusEnumVal",
                       None) != "S_CONFIRMED" and has_WM_ORDLIN_container(shipment):
                filteredShipments.append(shipment)

        print("filtered_enriched_shipment -> filteredShipments")
        print(filteredShipments)

        return filteredShipments

    def entityId(payload_shipment):
        transactionalReferences = payload_shipment.transactionalReference
        entityIds = map(lambda x: x.entityId, transactionalReferences)
        return list(entityIds)

    def extract_enriched_shipments(payload_shipment, enriched_shipments):
        shipments = [shipment for shipment in enriched_shipments if
                     shipment.shipmentNumber in entityId(payload_shipment)]
        return shipments

    # mapping logic
    containers = []
    payload_shipment_items = filtered_payload_shipment_items(payload.warehousingOutboundNotification)
    enriched_shipments = filtered_enriched_shipment(shipment)

    for payload_shipment in payload_shipment_items:
        enriched_shipments_for_current_payload = extract_enriched_shipments(payload_shipment, enriched_shipments)
        current_containers = flatten(map(lambda x: x.container, enriched_shipments_for_current_payload))
        current_containers = filter(is_WM_ORDLIN_container, current_containers)
        for c in current_containers:
            containers.append(
                SimpleNamespace(
                    mapped_data=SimpleNamespace(
                        scaledWeight=c.containerShippingInformation.scaledWeight,
                        orderValue=c.containerShippingInformation.orderValue,
                        declaredValue=c.containerShippingInformation.declaredValue,
                        tareWeight=c.containerShippingInformation.tareWeight,
                        pieces=c.containerShippingInformation.pieces,
                        skids=c.containerShippingInformation.skids,
                        ladenLength=c.containerShippingInformation.ladenLength,
                        input_qty=payload_shipment.plannedDespatchQuantity.value,
                        volume=calculate_volume(payload_shipment),
                        input_unit_weight=calculate_weight(payload_shipment),
                        numberOfUnits=c.numberOfUnits,
                        freightClassCode=c.weightByFreightClass[0].freightClassCode,
                        original_data=c
                    )
                )
            )

    return containers


def map_shipment_to_reference_numbers(shipment, payload, global_vars):
    import types
    # helper functions
    def flatten(iterables):
        import itertools
        return itertools.chain.from_iterable(iterables)

    def entityId(payload_shipment):
        transactionalReferences = payload_shipment.transactionalReference
        entityIds = map(lambda x: x.entityId, transactionalReferences)
        return list(entityIds)

    def is_shipment_leg_in_payload(enriched_shipment, payload_shipment):
        entity_ids = entityId(payload_shipment)
        shipment_legs = enriched_shipment.shipmentLeg
        system_shipment_leg_id = map(lambda x: x.systemShipmentLegID, shipment_legs)
        for leg_id in system_shipment_leg_id:
            if leg_id in entity_ids:
                return True
        return False

    def extract_enriched_shipment(payload_shipment, enriched_shipments):
        for shipment in enriched_shipments:
            if shipment.shipmentNumber in entityId(payload_shipment) or is_shipment_leg_in_payload(shipment,
                                                                                                   payload_shipment):
                return shipment

    def extract_WM_SHIPMENT_reference_numbers(enriched_shipments):
        reference_numbers = flatten(map(lambda x: x.referenceNumberStructure, enriched_shipments))
        reference_numbers = list(filter(lambda x: x.referenceNumberTypeCode == 'WM_SHIPMENT', reference_numbers))
        if len(reference_numbers) > 0:
            return reference_numbers[0].systemReferenceNumberID

    def get_reference_numbers_based_on_won_status_code(payload_shipment):
        switcher = {
            "STOCK_ALLOCATED": "WAVED",
            "PICK_RELEASED": "Pick Released",
            "PRODUCT_PICKED": "Pick Completed"
        }
        return switcher[payload_shipment.warehousingOutboundStatusCode]

    # mapping_logic
    reference_number_update_data = []
    payload_shipments = flatten(map(lambda x: x.shipment, payload.warehousingOutboundNotification))
    enriched_shipments = list(flatten(shipment))

    for payload_shipment in payload_shipments:
        enriched_shipment = extract_enriched_shipment(payload_shipment, enriched_shipments)
        system_reference_number_id = extract_WM_SHIPMENT_reference_numbers(enriched_shipments)
        reference_number_action_enum_val = "AT_UPDATE" if system_reference_number_id else "AT_ADD"
        reference_number = get_reference_numbers_based_on_won_status_code(payload_shipment)
        reference_number_update_data.append(
            types.SimpleNamespace(
                mapped_data=types.SimpleNamespace(
                    reference_number_entity_key=enriched_shipment.systemShipmentID,
                    system_reference_number_id=system_reference_number_id,
                    reference_number_action_enum_val=reference_number_action_enum_val,
                    reference_number=reference_number
                )

            )
        )
    return reference_number_update_data


def map_load_to_stop_planning_status_enum_val(load, payload, global_vars):
    # helper functions
    def flatten(iterables):
        import itertools
        return itertools.chain.from_iterable(iterables)

    def has_reference_numbers(shipment_leg, reference_number_type):
        reference_number_structures = shipment_leg.shipment.referenceNumberStructure
        reference_number_structures = list(
            filter(lambda x: x.referenceNumberTypeCode == "WM_SHIPMENT", reference_number_structures))
        if len(reference_number_structures) > 0 and reference_number_structures[
            0].referenceNumber == reference_number_type:
            return True
        return False

    def get_shipments(shipment_type, load, shipping_location):
        shipment_legs = flatten(map(lambda x: x.shipmentLeg, load))
        shipment_legs = filter(lambda x: has_reference_numbers(x, shipment_type), shipment_legs)
        shipment_legs = filter(
            lambda x, shipping_location=shipping_location: x.shipFromLocationCode == shipping_location, shipment_legs)
        shipment_legs = filter(lambda x: x.systemShipmentLegID != None, shipment_legs)
        return list(shipment_legs)

    def get_stop_planning_status(load, shipping_location):
        stops = flatten(map(lambda x: x.stop, load))
        stops = list(
            filter(lambda x, shipping_location=shipping_location: x.shippingLocationCode == shipping_location, stops))
        if len(stops) > 0:
            return stops[0].stopPlanningStatusEnumVal[0]

    enriched_load = list(flatten(load))
    warehousing_outbound_status_code = payload.warehousingOutboundNotification[0].shipment[
        0].warehousingOutboundStatusCode
    shipping_location = payload.warehousingOutboundNotification[0].shipment[0].shipFrom.primaryId
    stop_planning_status = get_stop_planning_status(enriched_load, shipping_location)

    count_of_picked_shipments = len(get_shipments("Pick Completed", enriched_load, shipping_location))
    count_of_waved_shipments = len(get_shipments("WAVED", enriched_load, shipping_location))

    if warehousing_outbound_status_code == "STOCK_ALLOCATION_UNSUCCESSFUL" and count_of_picked_shipments == 0:
        if count_of_waved_shipments > 0 and stop_planning_status != 'STPPLNG_STAT_PICKED':
            return 'STPPLNG_STAT_PICKED'
        elif count_of_waved_shipments == 0:
            return 'STPPLNG_STAT_DEFAULT'

    if warehousing_outbound_status_code == 'STOCK_ALLOCATED' and count_of_waved_shipments > 0 and count_of_picked_shipments < 1 and stop_planning_status != 'STPPLNG_STAT_PICKED':
        return 'STPPLNG_STAT_PICKED'

    if warehousing_outbound_status_code == 'PRODUCT_PICKED' and stop_planning_status != 'STPPLNG_STAT_LOCKED':
        return 'STPPLNG_STAT_LOCKED'


"""
 Functions for Receiving Advice
"""


def get_unique_load_receiving_advice(argument_list, payload, global_variable):
    unique_load_id = {}
    ra_document = []
    for item in eval('payload.receivingAdvice'):
        if eval('item.documentActionCode') != 'DELETE' and eval(
                'item.receivingAdviceTransportInformation.transportLoadId', None) is not None \
                and eval('item.receivingAdviceTransportInformation.transportLoadId') != "" \
                and unique_load_id.get(eval('item.receivingAdviceTransportInformation.transportLoadId'), None) is None:
            ra_document.append(item)
    return ra_document if len(ra_document) else None


def get_enriched_load_ra(argument_list, payload, global_variable):
    try:
        op = eval('payload.enrichment.load.data')
        return op if op is not None else None
    except:
        return None


def get_unique_load_item_receiving_advice(current_ra, payload, global_variable):
    unique_load_id = {}
    ra_document = []
    if eval('current_ra[0].receivingAdviceLogisticUnit') is not None:
        if eval('current_ra[0].receivingAdviceLogisticUnit[0].lineItem') is not None:
            for lineitem in eval('current_ra[0].receivingAdviceLogisticUnit[0].lineItem'):
                if hasattr(lineitem, 'transportationShipmentId'):
                    ra_document.append(lineitem)

    return ra_document if len(ra_document) else None


def get_current_load_document_ra(argument_list, payload, global_variable):
    for tm_enriched_load in argument_list[0]:
        tm_enriched_load_id = int(eval('tm_enriched_load.systemLoadID'))
        if argument_list[1] == tm_enriched_load_id:
            print(tm_enriched_load)
            return tm_enriched_load
    return None


def get_shipment_leg_document_ra(argument_list, payload, global_variable):
    for shipment_leg in argument_list[0].shipmentLeg:
        if eval('shipment_leg.shipment.shipmentNumber') == argument_list[1]:
            return shipment_leg
    return None


def get_total_non_alloc_quantity_ra(argument_list, payload, global_variable):
    if hasattr(argument_list[0], 'shipment'):
        shipment_list = eval('argument_list[0].shipment')
        for curr_container in shipment_list:
            if hasattr(curr_container, 'referenceNumberStructure'):
                for ref_str in eval('curr_container.referenceNumberStructure'):
                    if eval('ref_str.ReferenceNumberTypeCode', None) is not None \
                            and eval('ref_str.ReferenceNumberTypeCode') == 'DCS_NON_ALLO' \
                            and eval('ref_str.ReferenceNumber', None) is not None \
                            and eval('ref_str.ReferenceNumber') is True:
                        if hasattr(curr_container, 'quantity'):
                            quantity = eval('curr_container.quantity')
                            return quantity if quantity != '' else 0
                return 0
    return None


def get_total_non_alloc_weight_ra(argument_list, payload, global_variable):
    if hasattr(argument_list[0], 'shipment'):
        shipment_list = eval('argument_list[0].shipment')
        for curr_container in shipment_list:
            if hasattr(curr_container, 'referenceNumberStructure'):
                for ref_str in eval('curr_container.referenceNumberStructure'):
                    if eval('ref_str.ReferenceNumberTypeCode', None) is not None \
                            and eval('ref_str.ReferenceNumberTypeCode') == 'DCS_NON_ALLO' \
                            and eval('ref_str.ReferenceNumber', None) is not None \
                            and eval('ref_str.ReferenceNumber') is True \
                            and eval('curr_container.quantity', None) is not None \
                            and eval('currentContainer.containerShippingInformation.nominalWeight', None) is not None:
                        quantity = eval('curr_container.quantity') * eval(
                            'currentContainer.containerShippingInformation.nominalWeight')
                        return quantity if quantity != '' else 0
                return 0
    return None


def get_total_quantity_received_ra(argument_list, payload, global_variable):
    unique_load_id = {}
    total_quantity = 0
    if eval('argument_list[0].receivingAdviceLogisticUnit') is not None:
        if eval('argument_list[0].receivingAdviceLogisticUnit[0].lineItem') is not None:
            for lineitem in eval('argument_list[0].receivingAdviceLogisticUnit[0].lineItem'):
                if hasattr(lineitem, 'transportationShipmentId') \
                        and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                        and eval('lineitem.transportationShipmentId') == argument_list[1]:
                    current_value = eval('lineitem.quantityReceived.value')
                    total_quantity += current_value if current_value is not None else 0

    return total_quantity


def get_shipping_location_document_ra(ship_to_location_code, payload, global_variable):
    try:
        shipping_locations = eval('payload.shippingLocation.data')
        for shipping_location in shipping_locations:
            if hasattr('shipping_location', 'shipTo') and eval('shipping_location.shipTo') == ship_to_location_code[0]:
                return shipping_location
    except Exception as ex:
        return None
    return None


def get_total_shipment_weight_ra(argument_list, payload, global_variable):
    from neo.process.transformation.utils.conversion_util import ConversionUtils
    def get_total_quantity_received_ra(argument_list, payload, global_variable):
        unique_load_id = {}
        total_quantity = 0
        if eval('argument_list[0][0].receivingAdviceLogisticUnit') is not None:
            if eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem') is not None:
                for lineitem in eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem'):
                    if hasattr(lineitem, 'transportationShipmentId') \
                            and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                            and eval('lineitem.transportationShipmentId') == argument_list[1]:
                        current_value = eval('lineitem.quantityReceived.value')
                        total_quantity += current_value if current_value is not None else 0

        return total_quantity

    def get_total_weight_received_ra(argument_list, payload, global_variable):
        unique_load_id = {}
        total_quantity = 0
        if eval('argument_list[0][0].receivingAdviceLogisticUnit') is not None:
            if eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem') is not None:
                for lineitem in eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem'):
                    if hasattr(lineitem, 'transportationShipmentId') \
                            and unique_load_id.get(eval('lineitem.transportationShipmentId'), None) is None \
                            and eval('lineitem.transportationShipmentId') == argument_list[1]:
                        print(lineitem)
                        current_value = eval(
                            'lineitem.transactionalTradeItem.transactionalItemData[0].transactionalItemWeight[0].measurementValue.value')
                        total_quantity += current_value if current_value is not None else 0

        return total_quantity

    current_item = eval('argument_list[0][0].receivingAdviceLogisticUnit[0].lineItem[0]', None)
    totalWeight = get_total_weight_received_ra(argument_list, payload, global_variable)
    target_unit = argument_list[2]
    if current_item is not None and hasattr(current_item, 'transportationShipmentId'):
        if eval('current_item.transportationShipmentId') == argument_list[1]:
            source_unit = eval(
                'current_item.transactionalTradeItem.transactionalItemData[0].transactionalItemWeight[0].measurementValue.measurementUnitCode',
                None)
            if not source_unit and source_unit == 'UNIT_NET_WEIGHT':
                totalQuantityReceived = get_total_quantity_received_ra(argument_list, payload, global_variable)
                calculated_wight = ConversionUtils.getUomConversion("Weight", source_unit, target_unit,
                                                                    totalQuantityReceived * totalWeight)
                return calculated_wight
            else:
                calculated_wight = ConversionUtils.getUomConversion("Weight", source_unit, target_unit,
                                                                    totalWeight)
                return calculated_wight
    return None


def get_total_weight_ra(argument_list):
    if argument_list[0] is not None and argument_list[1] is not None:
        return f'{argument_list[0]} + {argument_list[1]}'
    return None


def get_input_shipping_loc_code_ra(system_load, payload, global_variable):
    if hasattr(system_load[0], 'systemLoadID'):
        system_load_id = getattr(system_load[0], 'systemLoadID')
    else:
        return None
    for r_a in eval('payload.receivingAdvice'):
        ra_transport_id = eval('r_a.receivingAdviceTransportInformation.transportLoadId')
        if ra_transport_id == system_load_id:
            return eval('r_a.shipTo.primaryId')
    return None


def get_load_reference_number_id_ra(load_document, payload, global_variable):
    if load_document[0] is not None and eval('load_document[0].referenceNumberStructure') is not None:
        for rns in eval('load_document[0].referenceNumberStructure'):
            if eval('rns.referenceNumberTypeCode') == "WM_WH_STS_":
                return eval('rns.systemReferenceNumberID')
    return None


def get_stop_shipping_location_code_ra(argument_list, payload, global_variable):
    if argument_list[0] is not None and eval('argument_list[0].stop') is not None:
        for stop in eval('argument_list[0].stop'):
            if eval('stop.shippingLocationCode') == argument_list[1]:
                return eval('stop.shippingLocationCode')[0]
    return None


def get_stop_reference_number_id_ra(load_document, payload, global_variable):
    if load_document[0] is not None and eval('load_document[0].referenceNumberStructure') is not None:
        for rns in eval('load_document[0].referenceNumberStructure'):
            if eval('rns.referenceNumberTypeCode') == "WM_SHP_STS":
                return eval('rns.systemReferenceNumberID')
    return None


def get_reference_number_action_enum_for_load_ra(args):
    stopReferenceNumberID = args[0]
    if stopReferenceNumberID not in [None, '']:
        return "AT_ADD"
    return args[1]


"RA Config end"


def get_uniqueupd_transportid(argument_list, payload, global_variable):
    from neo.exceptions import DataException
    ta_document = []
    if getattr(payload, "enrichment") is None:
        return None
    else:
        enrichment = getattr(payload, "enrichment")
        docktype_list = getattr(enrichment, 'dockType')
        for dock_dict in docktype_list:
            dockCommitment = getattr(dock_dict, 'dockCommitment', None)
            if dockCommitment is None:
                continue
            for curr_doc in dockCommitment:
                systemDockCommitmentId = getattr(curr_doc, 'systemDockCommitmentID', None)
                DockSlotIdentifier = getattr(dock_dict, 'dockSlotIdentifier', None)
        for item in eval('payload.transportPickUpDropOffConfirmation'):
            if eval('item.documentActionCode') == 'CHANGE_BY_REFRESH' and eval(
                    'item.transportPickUpDropOffConfirmationId') is not None:
                plannedDropOffDockSlotIdentifier = eval(
                    'item.plannedDropOff.logisticLocation.sublocationId')
                plannedPickUpDockSlotIdentifier = eval(
                    'item.plannedPickUp.logisticLocation.sublocationId')
                if plannedDropOffDockSlotIdentifier is not None:
                    dockSlotIdentifier = plannedDropOffDockSlotIdentifier
                elif plannedPickUpDockSlotIdentifier is not None:
                    dockSlotIdentifier = plannedPickUpDockSlotIdentifier
                else:
                    dockSlotIdentifier = None
                dockCommitmentID = eval('item.transportPickUpDropOffConfirmationId')
                enrichedDockCommitmentID = dockCommitmentID + systemDockCommitmentId
                systemDockID = dockSlotIdentifier + DockSlotIdentifier
            if enrichedDockCommitmentID is None and systemDockID is None:
                raise DataException(
                    f"Either systemdockcommitment:{enrichedDockCommitmentID} or systemdockid:{systemDockID} is having incorrect data")
            else:
                ta_document.append(item)
        return ta_document if len(ta_document) else None


def get_uniquedel_transportid(argument_list, payload, global_variable):
    ta_document1 = []
    for item in eval('payload.transportPickUpDropOffConfirmation'):
        if eval('item.documentActionCode') == 'DELETE' and eval(
                'item.transportPickUpDropOffConfirmationId') is not None:
            ta_document1.append(item)
    return ta_document1 if len(ta_document1) else None


def get_dockslot(*args):
    if args[0][0] is not None:
        return args[0][0]
    elif args[0][1] is not None:
        return args[0][1]
    else:
        return None


def get_olddockslot(argument_list, payload, global_variable):
    enrichment = getattr(payload, "enrichment")
    docktype_list = getattr(enrichment, 'dockType')
    for dock_dict in docktype_list:
        dockCommitment = getattr(dock_dict, 'dockCommitment', None)
        if dockCommitment is None:
            continue
        for curr_doc in dockCommitment:
            systemDockCommitmentId = getattr(curr_doc, 'systemDockCommitmentID', None)
            dockCommitmentId = global_variable.get('dockCommitmentId')
            if systemDockCommitmentId == dockCommitmentId:
                return getattr(dock_dict, 'dockSlotIdentifier', None)
    return None


def get_oldSystemDockId(argument_list, payload, global_variable):
    enrichment = getattr(payload, "enrichment")
    docktype_list = getattr(enrichment, 'dockType')
    for dock_dict in docktype_list:
        dockCommitment = getattr(dock_dict, 'dockCommitment', None)
        if dockCommitment is None:
            continue
        for curr_doc in dockCommitment:
            systemDockCommitmentId = getattr(curr_doc, 'systemDockCommitmentID', None)
            dockCommitmentId = global_variable.get('dockCommitmentId')
            if systemDockCommitmentId == dockCommitmentId:
                return getattr(dock_dict, 'systemDockID', None)
    return None


def get_locationOffset_ta(argument_list, payload, global_variable):
    enrichment = getattr(payload, "enrichment")
    docktype_list = getattr(enrichment, 'dockType')
    for dock_dict in docktype_list:
        dockCommitment = getattr(dock_dict, 'dockCommitment', None)
        if dockCommitment is None:
            continue
        for curr_doc in dockCommitment:
            systemDockCommitmentId = getattr(curr_doc, 'systemDockCommitmentID', None)
            dockCommitmentId = global_variable.get('dockCommitmentId')
            if systemDockCommitmentId == dockCommitmentId:
                return eval('dock_dict.businessHours.timeZoneOffset')
    return None


def check_new_and_old_dockslot(argument_list, payload, global_variable):
    if global_variable.get('oldDockSlotIdentifier') == global_variable.get('newDockSlotIdentifier'):
        return global_variable.get('oldSystemDockId')
    else:
        enrichment = getattr(payload, "enrichment")
        docktype_list = getattr(enrichment, 'dockType')
        for dock_dict in docktype_list:
            DockSlotIdentifier = getattr(dock_dict, 'dockSlotIdentifier', None)
            NewDockSlotIdentifier = global_variable.get('newDockSlotIdentifier')
            if DockSlotIdentifier is None:
                continue
            if DockSlotIdentifier == NewDockSlotIdentifier:
                return getattr(dock_dict, 'systemDockID', None)
    return None


# input : load stops , shipping location code
# return : evaluates reference number for load in won
def get_reference_number_for_load_won(args):
    load_stops = args[0]
    shipping_location_code = args[1]
    stopStatus = shipping_location_code + "AWARE"
    if not isinstance(load_stops, list):
        load_stops = [load_stops]
    loadStatus = ""
    for stop in load_stops:
        if getattr(stop, 'shippingLocationCode', -1) == shipping_location_code:
            if loadStatus == "":
                loadStatus = stopStatus
            else:
                loadStatus += ',' + stopStatus
        else:
            ref_num_struct = getattr(stop, 'referenceNumberStructure', None)
            if ref_num_struct is not None:
                ref_num = ""
                for ref in ref_num_struct:
                    if getattr(ref, 'referenceNumberTypeCode', -1) == 'WM_SHP_STS':
                        ref_num = getattr(ref, 'referenceNumber', '')
                        break

                if loadStatus == "":
                    loadStatus = ref_num
                else:
                    loadStatus += ',' + ref_num

    if len(loadStatus) > 30:
        loadStatus = loadStatus[:29]
    return loadStatus


# input: list of loads
# return: SystemReferenceNumberID from load.referenceNumberStructure
def get_load_reference_number_id_won(args, payload, global_vars):
    loads = args[0]
    referenceId = None
    if not isinstance(loads, list):
        loads = [loads]
    for load in loads:
        ref_num_struct = getattr(load, 'referenceNumberStructure', None)
        if ref_num_struct is not None:
            for ref in ref_num_struct:
                if getattr(ref, 'referenceNumberTypeCode', -1) == 'WM_WH_STS_':
                    referenceId = getattr(ref, 'systemReferenceNumberID', None)
                    return referenceId

    return referenceId


# input: list of loads
# return: SystemReferenceNumberID from stop.referenceNumberStructure
def get_stop_reference_number_id_won(args, payload, global_vars):
    ref_num_struct = args[0]
    shipping_location_code = args[1]
    won_shipping_location_code = args[2]
    if shipping_location_code != won_shipping_location_code:
        return ""

    referenceId = ""
    if ref_num_struct is not None:
        for ref in ref_num_struct:
            if getattr(ref, 'referenceNumberTypeCode', -1) == 'WM_SHP_STS':
                referenceId = getattr(ref, 'systemReferenceNumberID', None)
                return referenceId

    return referenceId


def get_reference_number_action_enum_for_load_won(args):
    loadReferenceNumberID = args[0]
    action_enum = None
    if loadReferenceNumberID == "" or loadReferenceNumberID is None:
        action_enum = "AT_ADD"
    else:
        action_enum = "AT_UPDATE"
    return action_enum


def get_reference_number_action_enum_for_stop_won(args):
    stopReferenceNumberID = args[0]
    action_enum = None
    if stopReferenceNumberID == "" or stopReferenceNumberID is None:
        action_enum = "AT_ADD"
    else:
        action_enum = "AT_UPDATE"
    return action_enum


def get_reference_number_for_stop_won(args):
    shippingLocationCode = args[0]
    return shippingLocationCode + "-AWARE"


def evaluate_load_planning_status_enum_val(args, payload, global_vars):
    loads = args[0]
    warehousingOutboundStatusCode = args[1]
    wavedShipments = []
    pickedShipments = []
    if not isinstance(loads, list):
        loads = [loads]

    shipmentLegs = getattr(loads[0], 'shipmentLeg')
    for sl in shipmentLegs:
        shipment = getattr(sl, 'shipment', None)
        if shipment is None:
            continue
        ref_num_struct = getattr(shipment, 'referenceNumberStructure', None)
        if ref_num_struct is None:
            continue
        for ref in ref_num_struct:
            if getattr(ref, 'referenceNumberTypeCode', "-1") == 'WM_SHIPMENT' and getattr(ref, 'referenceNumber',
                                                                                          "-1") == 'WAVED':
                wavedShipments.append(sl)
                continue
            elif getattr(ref, 'referenceNumberTypeCode', "-1") == 'WM_SHIPMENT' and getattr(ref, 'referenceNumber',
                                                                                            "-1") == 'Pick Completed':
                pickedShipments.append(sl)
                continue
    if len(wavedShipments) == 0:
        wavedShipments = None

    if len(pickedShipments) == 0:
        pickedShipments = None

    countOfWavedShipments = 0
    countOfPickedShipments = 0

    if wavedShipments is not None:
        countOfWavedShipments = len(wavedShipments)

    if pickedShipments is not None:
        countOfPickedShipments = len(pickedShipments)

    returnStatus = ""
    if warehousingOutboundStatusCode == "STOCK_ALLOCATION_UNSUCCESSFUL" and (countOfPickedShipments == 0) and (
            countOfWavedShipments > 0):
        return "PLNGSTS_PACKED"
    elif warehousingOutboundStatusCode == "STOCK_ALLOCATION_UNSUCCESSFUL" and (countOfWavedShipments == 0) and (
            countOfPickedShipments == 0):
        return "PLNGSTS_PICKED"
    elif warehousingOutboundStatusCode == "PICKING_UNSUCCESSFUL" and (countOfPickedShipments == 0) and (
            countOfWavedShipments == 0):
        return "PLNGSTS_PICKED"
    elif warehousingOutboundStatusCode == "STOCK_ALLOCATED" and (countOfWavedShipments > 0) and (
            countOfPickedShipments < 1):
        return "PLNGSTS_PACKED"
    elif warehousingOutboundStatusCode == "PRODUCT_PICKED":
        return "PLNGSTS_LOCKED"
    else:
        return None

