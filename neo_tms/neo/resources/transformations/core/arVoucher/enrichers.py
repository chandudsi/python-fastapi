from neo.log import Logger as logger
from neo.exceptions import DataException
from neo.connectors.aeh import AEHMessage
from neo.connectors.kafka import Message
from neo.connect.cps import adapter_cps_provider
import json
import copy


"""
Use Case:
  Used this function for forming the arvoucher enrichment call.
"""
def get_arvoucher_enrichment_api_call(payload, enriched_message):

    if isinstance(payload, AEHMessage) or isinstance(payload, Message):
        msg_value = json.loads(payload.value)
        payload = msg_value

    arvoucher_payload = payload.get('arVoucher', {})

    for arvoucher_data in arvoucher_payload:
        arVoucherDateTime=arvoucher_data.get("dateTime")

    arvoucher_api_call = {
		'select': {
			'name': [
				'createdDateTime',
				'updatedDateTime',
				'systemVoucherID',
				'initialVoucherID',
				'voucherType',
				'effectiveDate',
				'costCenterCode',
				'profitCenterCode',
				'currentStatusEnumVal',
				'heldFlag',
				'holdFlag',
				'ARVoucherLevelEnumVal',
				'totalVoucherRatingAmount',
				'ratingCurrencyCode',
				'totalVoucherPaymentAmount',
				'currencyCode',
				'totalTaxAmount',
				'internalReferenceNumber',
				'internalReferenceNumberTypeEnumVal',
				'invoiceNumber',
				'originalInvoiceNumber',
				'divisionCode',
				'logisticsGroupCode',
				'customerCode',
				'billToCustomerCode',
				'shipmentNumber',
				'commodityCode',
				'serviceCode',
				'originShippingLocationCode',
				'originShippingLocationTypeEnumVal',
				'destinationShippingLocationCode',
				'destinationShippingLocationTypeEnumVal',
				'memo'
			],
			'collection': [
				{
					'name': 'VoucherCharge',
					'select': {
						'name': [
							'systemChargeDetailID',
							'chargeLevelEnumVal',
							'chargeDetailEnumVal',
							'currentStatusEnumVal',
							'chargeCode',
							'reasonCode',
							'chargeUnits',
							'unitRate',
							'discountAmount',
							'originalCurrencyCode',
							'chargedAmount',
							'totalTaxAmount',
							'paymentAmount',
							'freightClassCode',
							'equipmentTypeCode',
							'systemLoadID',
							'systemShipmentID',
							'systemShipmentLegID',
							'systemContainerID',
							'prePurchasedFlag',
							'tripLevelChargeFlag'
						]
					}
				}
			]

		},
	'condition': {
		'o': [
			{
				'and': {
					'o': [
						{
							'or': {
								'o': [
									{
										'ge': {
											'o': [
												{
													'name': 'createdDateTime'
												},
												{
													'value': arVoucherDateTime
												}
											]
										}
									},
									{
										'ge': {
											'o': [
												{
													'name': 'updatedDateTime'
												},
												{
													'value': arVoucherDateTime
												}
											]
										}
									}
								]
							}
						},
						{
							'eq': {
								'o': [
									{
										'name': 'heldFlag'
									},
									{
										'value': 'false'
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
    return arvoucher_api_call




"""
Use Case:
  Used this function for forming appending the actual
  message with enriched message
"""
def form_arvoucher_tms_payload(entity, cfg, message, trigger, trigger_options, enricher_options,
                                      enriched_messages=None, message_hdr_info = {}):

    if isinstance(message, AEHMessage) or isinstance(message, Message):
        if isinstance(message.value, str):
            msg_value = json.loads(message.value)
        else:
            msg_value = message.value
        message_val = msg_value
        message_val['enrichment'] = enriched_messages
        message.value = message_val
    else:
        # message.update(enriched_messages)
        message['enrichment'] = enriched_messages
    return None, None
