order_payload = {
    "order": {
        "fulfillment_status": "fulfilled",
        "line_items": [
            {
                "variant_id": "45005569818897",
                "product_id": "8325169119505",
                "quantity": 1,
            }
        ],
        "customer": {
            "first_name": "Test",
            "last_name": "DSB",
            "email": "datascience+python@junodx.com"
        },
        "email": "datascience+python@junodx.com",
        "phone": "480-213-1212",
        "billing_address": {
            "first_name": "Test",
            "last_name": "Shopifyzer",
            "address1": "117 Ash Ave",
            "phone": "480-213-1212",
            "city": "Wood Village",
            "province": "Oregon",
            "country": "US",
            "zip": "97060"
        },
        "shipping_address": {
            "first_name": "Test",
            "last_name": "Shopifyzer",
            "address1": "117 Ash Ave",
            "phone": "480-213-1212",
            "city": "Wood Village",
            "province": "Oregon",
            "country": "US",
            "zip": "97060"
        },
        "financial_status": "paid",
        "test": True,
        "transactions": [
            {
                "test": True,
                "kind": "authorization",
                "status": "success",
                "amount": 200,
            }
        ]
    }
}


order_payload_2 = {
    "order": {
        "fulfillment_status": "fulfilled",
        "line_items": [
            {
                "variant_id": "45005569818897",
                "product_id": "8325169119505",
                "quantity": 1,
            }
        ],
        "customer": {
            "first_name": "Test",
            "last_name": "DSB",
            "email": "datascience+python@junodx.com"
        },
        "email": "datascience+python@junodx.com",
        "phone": "480-213-1212",
        "tags": "Sync: Failed,",
        "billing_address": {
            "first_name": "Test",
            "last_name": "DSB-Billing",
            "address1": "117 Ash Ave",
            "phone": "480-213-1212",
            "city": "Wood Village",
            "province": "Oregon",
            "country": "US",
            "zip": "97060"
        },
        "shipping_address": {
            "first_name": "Test",
            "last_name": "DSB",
            "address1": "117 Ash Ave",
            "phone": "480-213-1212",
            "city": "Wood Village",
            "province": "Oregon",
            "country": "US",
            "zip": "97060"
        },
        "financial_status": "paid",
        "test": True,
    }
}