def handle_condition(param):
    if len(param) == 2:
        param = ''.join(param)
    else:
        param = param[0]
    res = []
    if '=' in param:
        res = param.split('=')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '=', res[1]]
    elif '<' in param:
        res = param.split('<')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '<', res[1]]
    elif '>' in param:
        res = param.split('>')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '>', res[1]]
    elif '<=' in param:
        res = param.split('<=')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '<=', res[1]]
    elif '>=' in param:
        res = param.split('>=')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '>=', res[1]]
    elif '<>' in param:
        res = param.split('<>')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '<>', res[1]]
    elif '!=' in param:
        res = param.split('!=')
        res = [x.strip() for x in res if x != '']
        res = [res[0], '!=', res[1]]
    else:
        raise TypeError('Invalid comparison operator. Please enter valid comparison operator.')
    return res
