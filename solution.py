from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import dateutil.parser
import time
import certifi
ca = certifi.where()


def get_database():
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://abderzak:abderzak@cluster0.rqtdt3c.mongodb.net/?retryWrites=true&w=majority"
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
            result = datas.find({"station_id" : elt["_id"]}).sort("date",-1).limit(1)
            if result[0]["bike_availbale"] >= 1 :
                print("vélo Disponible : ",result[0]["bike_availbale"])
                print("Place Disponible : ",result[0]["stand_availbale"])
                print("Date : ", result[0]["date"])
            else:
                print("Plus aucun vélo est disponible, nous somme désolé")
                print("vélo Disponible : ",result[0]["bike_availbale"])
                print("Place Disponible : ",result[0]["stand_availbale"])
                print("Date : ", result[0]["date"])
    
    #Question 4
    
    elif choix == '2':

        while True:

            print("\n---------------------------------Menu DB---------------------------------\n")
            print("TAPER [SEARCH] pour rechercher une station")
            print("TAPER [UPDATE] pour mettre à jour une station")
            print("TAPER [DELETE] pour supprimer une station")
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

            elif decision == "QUIT":
                break


            else:
                print("Commande inconnue")
    
    else:
        print("Mauvais choix")
#2,98
#50,61
