
def serialize_base(base):
    return {
        "value": base['id'],
        "label":  base['name'],
        "sample":  base['name']
    }


def serialize_table(table):
    return {
        "value": table['name'],
        "label":  table['name'],
        "sample":  table['id']
    }
