# -*- coding: utf-8 -*-
import logging

class AvroSchemaToJsonSchema:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info('Initialised')

    def _convert_field(self, avro_field, json_schema):
        fname = field['name']
        is_required = True
        ftype = 'string'

        if isinstance(field['type'], list):
            is_required = False
        else:
            ftype = field['type']['type']

        json_schema['properties'][fname] = {
            'type': ftype
        }
        json_schema['required'].append(fname)


    def convert(self, avro_schema):
        json_schema = {
            'title': avro_schema['name'],
            'type': 'object',
            'properties': {},
            'required': []
        }

        for field in avro_schema['fields']:
            self._convert_field(field, json_schema)

        return json_schema


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    logger = logging.getLogger(__name__)

    avro_schema = {
        "fields": [
            {
                "name": "query_type",
                "type": {
                    "type": "string"
                }
            },
            {
                "name": "response_name",
                "type": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "string"
                    }
                ]
            },
            {
                "name": "timestamp",
                "type": {
                    "type": "long"
                }
            },
            {
                "name": "rrsig_signature_expiration",
                "type": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "long"
                    }
                ]
            },
            {
                "name": "rrsig_key_tag",
                "type": [
                    {
                        "type": "null"
                    },
                    {
                        "type": "int"
                    }
                ]
            }
        ],
        "name": "DNSEvoDataFlatAvro",
        "namespace": "nl.utwente.dacs.DNSEvo.avro",
        "type": "record"
    }

    converter = AvroSchemaToJsonSchema()
    json_schema = converter.convert(avro_schema)
    logger.info(json_schema)
