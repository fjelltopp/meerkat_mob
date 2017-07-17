

""" 
GCM messages have the actual payload in the 'data' object of the message. To make querying from DynamoDB easier, subobjects in
'data' are raised one step in the object hierarchy. In addition, DynamoDB expects the data to be contained in a datatype
dictionary
"""
def wrap_json_data(msg):
    flattened_msg = {}

    for i in msg.keys():
        if i != 'data':
            flattened_msg[i] = msg[i]
        else:
            for j in msg['data'].keys():
                flattened_msg[j] = msg['data'][j]

    final_msg = wrap_data_types(flattened_msg)
    return final_msg['M']

""" 
DynamoDB requires each value to be fed as a dictionary of data type code and value. 
These are recursively added in this function.
"""
def wrap_data_types(msg):

    if type(msg) is str:
        return {'S':msg}
    elif type(msg) is int:
        return {'N':str(msg)}
    elif type(msg) is list:
        value_list = []
        for item in msg:
            value_list.append(wrap_data_types(item))
        return {'L':value_list}
    elif type(msg) is dict:
        value_dict = {}
        for key in msg.keys():
            value_dict[key] = wrap_data_types(msg[key])
        return {'M':value_dict}


""" 
DynamoDB stores data in dictionaries of data type code and value. 
This structure is recursively unpacked in this function
"""
def unwrap_data_types(msg):
    retval = {}
    for key in msg.keys():
        if 'M' in msg[key].keys():
            for subkey in msg[key]['M'][subkey]:
                retval[key].update({subkey:unwrap_data_types(msg[key][subkey])})
        elif 'L' in msg[key].keys():
            for item in msg[key]['L']:
                retval[key].append(unwrap_data_types(item))
        elif 'S' in msg[key].keys():
            retval[key]=msg[key]['S']
        elif 'N' in msg[key].keys():
            retval[key]=msg[key]['N']
    return retval