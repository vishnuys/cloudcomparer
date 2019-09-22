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
        raise SyntaxError('Invalid comparison operator. Please enter valid comparison operator.')
    return res


table_row_mapper = {
    'users': {'userid': 0, 'age': 1, 'gender': 2, 'occupation': 3, 'zipcode': 4},
    'zipcodes': {'zipcode': 0, 'zipcodetype': 1, 'city': 2, 'state': 3},
    'rating': {'userid': 0, 'movieid': 1, 'rating': 2, 'timestamp': 3},
    'movies': {'movieid': 0, 'title': 1, 'releasedate': 2, 'unknown': 3, 'Action': 4, 'Adventure': 5, 'Animation': 6, 'Children': 7, 'Comedy': 8, 'Crime': 9, 'Documentary': 10, 'Drama': 11, 'Fantasy': 12, 'Film_Noir': 13, 'Horror': 14, 'Musical': 15, 'Mystery': 16, 'Romance': 17, 'Sci_Fi': 18, 'Thriller': 19, 'War': 20, 'Western': 21}
}
