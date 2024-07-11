from neo.log import Logger as logger


def check_transport_load(*args):
    event_name = args[0][0]
    load_id = args[0][1]

    accepted_events = ['LoadTenderAccepted', 'LoadTenderUpdated', 'LoadTenderCancelled', 'LoadStopAppointment', 'LoadRoutedRatedScheduled']
    if event_name in accepted_events and load_id is not None:
        logger.debug(f"entity identification done : Transport Load with Load id({load_id})")
        return True
    return False


def check_shipment_leg_operation(*args):
    event_name = args[0][0]
    shipmentLegId = args[0][1]

    accepted_events = ['ShipmentLegRemovedFromLoad', 'ShipmentLegAddedToPlannedLoad']
    if event_name in accepted_events and shipmentLegId is not None:
        logger.debug(f"entity identification done : Shipment Leg Operation with Shipment Leg Id({shipmentLegId})")
        return True
    return False
