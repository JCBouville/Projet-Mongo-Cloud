from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import dateutil.parser
import time
import certifi
import math
ca = certifi.where()


def get_database():
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://jeanc:N3WPtRWBV7SGffi4@cluster0.vc8v6x8.mongodb.net/?retryWrites=true&w=majority"
   # Create a connection using MongoClient. 
   client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'),tlsCAFile=ca)
   return client['vls']

def get_hexagon(center, radius):
    result=[]
    for i in range(6):
        result.append([round(center[0] + radius * math.cos(i * math.pi / 3),4), round(center[1] + radius * math.sin(i * math.pi / 3),4)])
    result.append(result[0])
    return result


if __name__ == "__main__": 
    db_name = get_database()
    datas = db_name["datas"]
    stations = db_name["stations"]

    # #Creation index 2D sphere 

    stations.create_index([("geometry" , "2dsphere")])
    
    print("\n---------------------------------Bienvenue cher client---------------------------------\n")
    choix = input("TAPER [1] pour trouver une station TAPER [2] pour ouvrir le menu de navigation de la DB \n\n")
    
    if choix == "1":
        print("---------------------------------Vous allez entrer vos coordonnées---------------------------------\n\n")
        lon = input("Entrez votre longitude : ")
        lat = input("Entrez votre latitude : ")

        lon = float(lon.replace(',','.'))
        lat = float(lat.replace(',','.'))

    #     # Trouve les stations les plus proche
        nearest_station = stations.find({"$and":[{
                    'geometry': {
                        "$near": { 
                            "$geometry": { 
                                "type": "Point" , 
                                "coordinates": [ lon, lat ]
                            },
                            "$maxDistance": 1000,"$minDistance": 0
                        }
                    }
                
                },
                {
                    "deactivated":{"$eq":False}
                }]})

    # lastdata et mettre un index sur les stations id et les heures
        datas.create_index("station_id")
        datas.create_index([("date",1)],unique=False)
    
        for elt in nearest_station:
            print("Station proche : ", elt["name"])
            result = datas.find({"station_id" : elt["_id"]}).sort("date",-1).limit(1)[0]
            if not result["bike_availbale"] >= 1 :
                print("Plus aucun vélo est disponible, nous sommes désolé")
            print("vélos Disponibles : ",result["bike_availbale"])
            print("Places Disponibles : ",result["stand_availbale"])
            print("Date : ", result["date"])
    
    #Question 4
    
    elif choix == '2':

        while True:

            print("\n---------------------------------Menu DB---------------------------------\n")
            print("TAPER [SEARCH] pour rechercher une station")
            print("TAPER [UPDATE] pour mettre à jour une station")
            print("TAPER [DELETE] pour supprimer une station")
            print("TAPER [DEACTIVATE] pour désactiver les stations dans une zone donnée")
            print("TAPER [GETRATIO] pour obtenir les stations possèdant un ratio vélo/supports totaux inférieur à 20% sur une plage horaire donnée les jours ouvrés")
            print("TAPER [QUIT] pour quitter le menu")

            decision = input("Entrez votre action\n")
            if decision == "SEARCH":
                nom_station_search=input("Quelle station cherchez-vous ?")
                resultat_recherche = stations.find({"$and":[{"name" : {"$regex" : nom_station_search,'$options' : 'i'}}, {"$deactivated":{"$eq":True}}]})
                for elt_search in resultat_recherche:
                    print("Nom Station : ",elt_search["name"])
        
            elif decision == "UPDATE":
                action = input("TAPER [nom] pour modifier le nom de la station, [TPE] pour changer sa valeur ou [ACTIVATE] pour la réactiver : ")
                if action == "nom":
                    nom_station_recherche = input("Donner le nom de la station : ")
                    nom_station_update = input("Donner le nouveau nom de la station : ")
                    resultat_nom_station_modifie = stations.update_one({"name" : nom_station_recherche},{"$set" : {"name" : nom_station_update}})
                
                elif action == "TPE":
                    nom_station_recherche = input("Donner le nom de la station : ")
                    tpe = input("Donner la valeur du TPE (true ou false) : ")
                    if tpe == "true" : 
                        stations.update_one({"name" : nom_station_recherche},{"$set" : {"tpe" : True}})
                    
                    elif tpe == "false" : 
                        stations.update_one({"name" : nom_station_recherche},{"$set" : {"tpe" : False}})
                    
                    else:
                        print("Erreur")
                elif action == "ACTIVATE":
                    entree = input("Si vous souhaitez toutes les réactiver, tapez Y. Sinon tapez entrée\n")
                    if entree == "Y":
                        stations.update_many({}, {"$set" : {"deactivated" : False}})
                    else:
                        nom_station_recherche = input("Donner le nom de la station : ")
                        stations.update_one({"name" : nom_station_recherche},{"$set" : {"deactivated" : False}})
                    print("Station réactivée")

                else:
                    print("Erreur sur le choix")

            elif decision == "DELETE":
                nom_station_a_supprimer = input("Donner le nom de la station à supprimer : ")
                stations.delete_one({"name" : nom_station_a_supprimer})

            elif decision == "DEACTIVATE":
                
                lon = input("longitude :")
                lat = input("latitude:")

                lon = float(lon.replace(',','.'))
                lat = float(lat.replace(',','.'))
                center = [lon, lat]
                radius = float(input("Dans quel rayon ? (en km)").replace(',','.'))/111 #Divisé par 111 pour convertir à peu près les kilomètres en radians

                stations.update_many({"geometry":{
                        "$geoWithin":{
                            "$geometry":{
                                "type":"Polygon",
                                "coordinates":[get_hexagon(center, radius)]
                            }
                        }
                    }}, {"$set" : {"deactivated" : True}})

            elif decision == "GETRATIO":
                lowerBound = int(input("De quelle heure ?\n"))
                upperBound = int(input("À quelle heure ?\n"))
                filteredStations = datas.aggregate([{
                    "$addFields":{
                        "ratio":{
                            "$cond":[
                                {"$eq":[{"$add":["$bike_availbale", "$stand_availbale"]}, 0]},
                                "N/A",
                                {"$divide":["$bike_availbale", {"$add":["$bike_availbale", "$stand_availbale"]}]}
                            ]
                        },
                        "hour":{
                            "$hour":"$date"
                        },
                        "minute":{
                            "$minute":"$date"
                        },
                        "second":{
                            "$second":"$date"
                        },
                        "dayWeek":{
                            "$switch":{
                                "branches":[
                                    {
                                        "case":{"$eq":[{"$dayOfWeek":"$date"}, 2]},
                                        "then":"lundi"
                                    },
                                    {
                                        "case":{"$eq":[{"$dayOfWeek":"$date"}, 3]},
                                        "then":"mardi"
                                    },
                                    {
                                        "case":{"$eq":[{"$dayOfWeek":"$date"}, 4]},
                                        "then":"mercredi"
                                    },
                                    {
                                        "case":{"$eq":[{"$dayOfWeek":"$date"}, 5]},
                                        "then":"jeudi"
                                    },
                                    {
                                        "case":{"$eq":[{"$dayOfWeek":"$date"}, 6]},
                                        "then":"vendredi"
                                    }
                                ],
                                "default":"toFilter"
                            }
                        }
                    }
                },
                {
                    "$lookup":{
                        "from":"stations",
                        "localField":"station_id",
                        "foreignField":"_id",
                        "as":"station"
                    }
                },
                {
                    "$unwind":"$station"
                },
                {
                    "$match":{
                        "$and":[
                            {"ratio":{"$ne":"N/A"}},
                            {"ratio":{"$lte":0.2}},
                            {"dayWeek":{"$ne":"toFilter"}},
                            {"$or":[
                                {"$and":[{"hour":{"$gte":lowerBound}}, {"hour":{"$lt":upperBound}}]},
                                {"$and":[
                                    {"hour":{"$eq":upperBound}},
                                    {"minute":{"$eq":0}},
                                    {"second":{"$eq":0}}
                                ]}
                            ]},
                            {"station.deactivated":{"$eq":False}}
                        ]
                    }
                },
                {
                    "$project":{
                        "name":"$station.name",
                        "dayWeek":"$dayWeek",
                        "hour":"$hour",
                        "minute":"$minute",
                        "second":"$second",
                        "bike_availbale":"$bike_availbale",
                        "stand_availbale":"$stand_availbale",
                        "ratio":"$ratio"
                    }
                }])

                for elt in filteredStations:
                    print(elt["name"],"jour:",elt['dayWeek'],"heure:", f"{elt['hour']}:{elt['minute']}:{elt['second']}", "bike_available:", elt['bike_availbale'],"stand_available:", elt['stand_availbale'],"ratio:", elt['ratio'])

            elif decision == "QUIT":
                break

            else:
                print("Commande inconnue")
    
    else:
        print("Mauvais choix")
#2,98
#50,61
