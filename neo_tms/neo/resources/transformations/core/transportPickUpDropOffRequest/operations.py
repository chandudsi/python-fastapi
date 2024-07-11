from datetime import datetime
import pytz

def get_gs1_location_offset(location_offset, default_tz):
    """
    This function adds two location offset and returns the resulted offset
    params- location_offset: business offset fetched for the shiping location
    params- default_tz: time zone of TM Adapter
    :return: resultant offset
    """
    if location_offset is None:
        location_offset = 0

    timezones = pytz.all_timezones

    if default_tz not in timezones:
        default_tz = "UTC"

    tm_tz = pytz.timezone(default_tz)
    tm_offset_minutes = tm_tz.utcoffset(datetime.now()).total_seconds() // 60
    location_offset = float(location_offset) * 60

    location_offset_in_minutes = float(tm_offset_minutes) + float(location_offset)
    location_offset_in_hours = location_offset_in_minutes//60
    remainder_minutes = location_offset_in_minutes % 60
    offset_sign = '-' if location_offset_in_hours < 0 else '+'
    offset_hour = "{:02}".format(int(abs(location_offset_in_hours)))
    offset_min = "{:02}".format(int(remainder_minutes))

    return f"{offset_sign}{offset_hour}:{offset_min}"


def getTimeComponentForTPDR(*args):
    """
    This function evaluates time component of TPDR
    params- date-component, location offset, tm-timezone
    :return: evaluated time component
    """
    actual_time = args[0][0]
    location_offset = args[0][1]
    tm_defaultTZ = args[0][2]

    offset = get_gs1_location_offset(location_offset, tm_defaultTZ)
    return actual_time + offset