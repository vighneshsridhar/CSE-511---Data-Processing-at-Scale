df = pd.DataFrame(data)
df.to_csv('data.csv')


def distance(lat2, lon2, lat1, lon1):
    R = 3959
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    lambda_phi = math.radians(lon2 - lon1)
    a = math.sin(delta_phi/2) * math.sin(delta_phi/2) + math.cos(delta_phi/2) * math.cos(delta_phi/2) * math.sin(lambda_phi/2) * math.sin(lambda_phi/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c
    return d

    pass




def FindBusinessBasedOnCity(cityToSearch,saveLocation1,collection):
    city = filter(lambda obj: obj['city'] == cityToSearch, collection)
    df = pd.DataFrame(city)
    print (df)

    with open(saveLocation1, 'w') as file:
        for line in city:
            file.write(line['name'] + "$" + line['full_address'] + "$" + line['city'] + "$" + line['state'] + "\n")

    pass

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    lat1 = myLocation[0]
    lon1 = myLocation[1]
    #df = pd.DataFrame(collection)
    #print (df)
    categoriesToSearch = set(categoriesToSearch)
    isCategory = categoriesToSearch.issubset
    in_category = filter(lambda obj: obj['categories'].map(isCategory), collection)
    df = pd.DataFrame(in_category)
    print (df)
    #collection['distance'] = map(lambda x, y: distance(x, y, lat1, lon1), collection['latitude'], collection['longitude'])
    #print (collection)

    pass
