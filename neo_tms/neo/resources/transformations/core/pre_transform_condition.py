from neo.connect.cps import adapter_cps_provider
from neo.log import Logger as logger
import inspect


def check_if_load_planning_status(*args):
    updateStopPlanningStatus = args[0][0]
    if updateStopPlanningStatus in [True, 'True', 'true']:
        return False
    return True


def check_if_stop_planning_status(*args):
    updateStopPlanningStatus = args[0][0]
    if updateStopPlanningStatus in [True, 'True', 'true']:
        return True
    return False
