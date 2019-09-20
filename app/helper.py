def handle_condition(param):
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
    return res
