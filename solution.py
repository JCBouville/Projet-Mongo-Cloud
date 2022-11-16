from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import dateutil.parser
import time
import certifi
ca = certifi.where()


def get_database():
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://jeanc:N3WPtRWBV7SGffi4@cluster0.vc8v6x8.mongodb.net/?retryWrites=true&w=majority"
   # Create a connection using MongoClient. 
   client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'),tlsCAFile=ca)
   return client['vls']


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
        nearest_station = stations.find({
                    'geometry': {
                        "$near": { 
                            "$geometry": { 
                                "type": "Point" , 
                                "coordinates": [ lon, lat ]
                            },
                            "$maxDistance": 1000,"$minDistance": 0
                        }
                    }
                
            })

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
                resultat_recherche = stations.find({"name" : {"$regex" : nom_station_search,'$options' : 'i'}})
                for elt_search in resultat_recherche:
                    print("Nom Station : ",elt_search["name"])
        
            elif decision == "UPDATE":
                action = input("TAPER [nom] pour modifier le nom de la station ou [TPE] pour changer sa valeur : ")
                if action == "nom":

                    nom_station_recherche = input("Donner le nom de la station à modifier : ")
                    nom_station_update = input("Donner le nouveau nom de la station : ")
                    resultat_nom_station_modifie = stations.update_one({"name" : nom_station_recherche},{"$set" : {"name" : nom_station_update}})
                
                elif action == "TPE":

                    nom_station_recherche = input("Donner le nom de la station : ")
                    tpe = input("Donner la valeur du TPE (true ou false) : ")
                    if tpe == "true" : 
                        resultat_tpe_modifie = stations.update_one({"name" : nom_station_recherche},{"$set" : {"tpe" : True}})
                    
                    elif tpe == "false" : 
                        resultat_tpe_modifie = stations.update_one({"name" : nom_station_recherche},{"$set" : {"tpe" : False}})
                    
                    else:
                        print("Erreur")

                else:
                    print("Erreur sur le choix")

            elif decision == "DELETE":
                nom_station_a_supprimer = input("Donner le nom de la station à supprimer : ")
                stations.delete_one({"name" : nom_station_a_supprimer})

            elif decision == "DEACTIVATE":
                pass
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
                            ]}
                        ]
                    }
                },
                {
                    "$project":{
                        "station_id":"$station_id",
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
                    print(list(stations.find({"_id":elt["station_id"]}))[0]["name"],"jour:",elt['dayWeek'],"heure:", f"{elt['hour']}:{elt['minute']}:{elt['second']}", "bike_available:", elt['bike_availbale'],"stand_available:", elt['stand_availbale'],"ratio:", elt['ratio'])

            elif decision == "QUIT":
                break


            else:
                print("Commande inconnue")
    
    else:
        print("Mauvais choix")
#2,98
#50,61
