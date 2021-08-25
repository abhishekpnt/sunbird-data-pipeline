"""
usage:

In [1]: from flatten import flattened_events
In [2]: import json
In [3]: event = json.loads(open('cb_audit.wat.json').read())
In [5]: flat_events = list(flattened_events(event))
In [6]: print(json.dumps(flat_events, indent=4))

"""


def merged_dict(dict_prefix_exclusions_list):
    """
    merge arbitrary number of dicts and return a new dict
    use single dict, to make a copy
    """
    dict_merged = {}
    for dict_i, prefix_i, exclusions_i in dict_prefix_exclusions_list:
        exclusions_i = set() if not exclusions_i else set(exclusions_i)
        for key, val in dict_i.items():
            if key not in exclusions_i:
                merged_key = key if not prefix_i else '%s%s' % (prefix_i, key)
                dict_merged[merged_key] = val
    return dict_merged


def flattened_events(event):
    cb_data = event.get('edata', {}).get('cb_data', {})
    # TO-MERGE: cb_data, prefix=None, exclude=['users']
    cb_data_users = cb_data.get('users', [])
    for cb_data_user in cb_data_users:
        # TO-MERGE: cb_data_user, prefix='users_', exclude=['roleCompetencyList']
        role_competency_list = cb_data_user['roleCompetencyList']
        for role_competency in role_competency_list:
            # TO-MERGE: role_competency, prefix='users_roleCompetencyList_', exclude=['roleDetails','competencyDetails']
            # exclude=['roleDetails','competencyDetails'] results in no fields from this level being merged
            role_details = role_competency['roleDetails']
            # TO-MERGE: role_details, prefix='users_roleCompetencyList_roleDetails_', exclude=['childNodes']

            # activities associated with this role
            role_child_nodes = role_details['childNodes']
            for child_node in role_child_nodes:
                # TO-MERGE: child_node, prefix='users_roleCompetencyList_roleDetails_childNodes_', exclude=[]
                yield merged_dict([
                    # dict              prefix                                              exclusion
                    (cb_data,           None,                                               ['users']),
                    (cb_data_user,      'users_',                                           ['roleCompetencyList']),
                    (role_competency,   'users_roleCompetencyList_',                        ['roleDetails', 'competencyDetails']),
                    (role_details,      'users_roleCompetencyList_roleDetails_',            ['childNodes']),
                    (child_node,        'users_roleCompetencyList_roleDetails_childNodes_', []),
                ])

            # competencies associated with this role
            competency_details = role_competency['competencyDetails']
            for competency in competency_details:
                # TO-MERGE: competency, prefix='users_roleCompetencyList_competencyDetails_', exclude=[]
                yield merged_dict([
                    # dict              prefix                                          exclusion
                    (cb_data,           None,                                           ['users']),
                    (cb_data_user,      'users_',                                       ['roleCompetencyList']),
                    (role_competency,   'users_roleCompetencyList_',                    ['roleDetails', 'competencyDetails']),
                    (role_details,      'users_roleCompetencyList_roleDetails_',        ['childNodes']),
                    (competency,        'users_roleCompetencyList_competencyDetails_',  []),
                ])

