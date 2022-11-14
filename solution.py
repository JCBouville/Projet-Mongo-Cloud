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
            print("TAPER [GETRATIO] pour obtenir les stations possèdant un ratio vélo/supports totaux inférieur à 20% entre 18h et 19h les jours ouvrés")
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
                #print(list(datas.aggregate([{"$project":{"hour":{"$hour":"$date"}, "minute":{"$minute":"$date"}, "second":{"$second":"$date"}}}, {"$match":{"hour": 18}}])))
                print(list(datas.aggregate([{"$project": {"dayOfWeek":{"$dayOfWeek":"$date"}}}])))
                #allStationIds = list(stations.distinct("_id"))
                #result = []
                #for i in allStationIds:
                #    datas.find({"station_id" : i, "$hour" : {"$"}})

            elif decision == "QUIT":
                break


            else:
                print("Commande inconnue")
    
    else:
        print("Mauvais choix")
#2,98
#50,61
