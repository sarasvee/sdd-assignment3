from DataParser import DataParser
from DbConnector import DbConnector
from DBWriter import DBWriter

import datetime
from haversine import haversine
from tabulate import tabulate

USER="User"
ACTIVITY="Activity"
TRACKPOINT="TrackPoint"

class Task2:
    def __init__(self):
        self.connection = DbConnector(DATABASE="my_db", HOST="localhost", USER="test_user", PASSWORD="test_password")
        self.client = self.connection.client
        self.db = self.connection.db
        self.db_writer = DBWriter(self.connection)
        self.parser = DataParser("sdd-assignment3/dataset")

    def task1(self):
        users_count= self.db[USER].count_documents({})
        print(f"User count: {users_count}")
        activity_count = self.db[ACTIVITY].count_documents({})
        print(f"Activity count: {activity_count}")
        track_point_count=self.db[TRACKPOINT].count_documents({})
        print(f"Trackpoint count: {track_point_count}")

    def task2(self):
        user_list=self.db[USER].find({})
        activity_counts = []
        for user in user_list:
            activity_counts.append(len(user["activities"]))
        
        average_activies_per_user= sum(activity_counts) / len(activity_counts)
        print(f"The average number of activities per user is {average_activies_per_user}")


    def task3(self):
        user_list=self.db[USER].find({})
        users_and_count = [(user["_id"], len(user["activities"])) for user in user_list]
        users_and_count.sort(key=lambda x : x[1], reverse=True)
        twenty_users_with_most_activities = [uac[0] for uac in users_and_count[:20]]
        print(twenty_users_with_most_activities)

    def task4(self):
        user_list=self.db[USER].find({})
        taxi_users = []
        for user in user_list:
            for activity_id in user["activities"]:
                user_activity = self.db[ACTIVITY].find_one({"_id": activity_id})
                if "taxi" ==user_activity["type"]:
                    taxi_users.append(user["_id"])
                    break
        print(taxi_users)


    def task5(self):
        acitivity_list=self.db[ACTIVITY].find({})
        activity_type_dict = {}
        for activity in acitivity_list:
            if activity["type"] in activity_type_dict.keys():
                activity_type_dict[activity["type"]] = activity_type_dict[activity["type"]] + 1
            else:
                activity_type_dict[activity["type"]] = 1
        
        print(activity_type_dict)

    def task6a(self):
        year_activity_dict = {}
        activity_list=self.db[ACTIVITY].find({})
        for activity in activity_list:
            start_year = activity["start_time"].year
            if start_year in year_activity_dict.keys():
                year_activity_dict[start_year] = year_activity_dict[start_year]+1
            else:
                year_activity_dict[start_year] = 1

        max_count = 0
        max_year = None
        for year, count in year_activity_dict.items():
            if count > max_count:
                max_year=year
                max_count=count
        
        print(max_year)


    def task6b(self):
        year_hour_dict={}
        activity_list = self.db[ACTIVITY].find({})
        for activity in activity_list:
            start_year = activity["start_time"].year
            hour_in_activity = activity["end_time"] - activity["start_time"]
            hour_in_activity = hour_in_activity.seconds / 3600
            if start_year in year_hour_dict.keys():
                year_hour_dict[start_year] = year_hour_dict[start_year] + hour_in_activity
            else:
                year_hour_dict[start_year] = hour_in_activity

        max_hour = 0
        max_year = None
        for year, count in year_hour_dict.items():
            if count > max_hour:
                max_year = year
                max_hour = count

        print(max_year)
    
    def task7(self):
        distance_walked=0
        user_activity_id_list= self.db[USER].find_one({"_id": "112"})["activities"]
        for activity_id in user_activity_id_list:
            activity = self.db[ACTIVITY].find_one({"_id": activity_id})
            if activity["type"] == "walk" and activity["start_time"].year == 2008:
                trackpoint_id_list = activity["trackpoints"]

                track_point_1 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[0]})

                for i in range(1, len(trackpoint_id_list)):
                    track_point_2 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[i]})
                    distance_walked += haversine((track_point_1["lat"], track_point_1["lon"]), (track_point_2["lat"], track_point_2["lon"]))

                    track_point_1 = track_point_2.copy()

        print(distance_walked)

    def task8(self):
        user_gained_alt_dict = {}
        user_list = self.db[USER].find({})
        
        for user in user_list:
            user_gained_alt_dict[user["_id"]] = 0

            for activity_id in user["activities"]:
                activity = self.db[ACTIVITY].find_one({"_id": activity_id})

                trackpoint_id_list = activity["trackpoints"]

                start_index = 0
                end_index = -1
                
                track_point_1 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[start_index]})
                while track_point_1["alt"] == -777:
                    start_index += 1
                    track_point_1 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[start_index]})

                track_point_2 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[end_index]})

                while track_point_2["alt"] == -777:
                    end_index -= 1
                    track_point_2 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[end_index]})

                gained_alt = track_point_2["alt"] - track_point_1["alt"]
                user_gained_alt_dict[user["_id"]] = user_gained_alt_dict[user["_id"]] + gained_alt
        
        key_value_list = [(key, value) for key, value in user_gained_alt_dict.items()]
        key_value_list.sort(key=lambda x: x[1], reverse=True)

        key_value_list= key_value_list[:20]
        print(tabulate(key_value_list, headers=["id", "altitude gained"]))

    
    def task9(self):
        user_and_number_of_invalid_activities_dict = {}
        user_list = self.db[USER].find({})

        for user in user_list:
            for activity_id in user["activities"]:
                activity = self.db[ACTIVITY].find_one({"_id": activity_id})
                trackpoint_id_list = activity["trackpoints"]

                track_point_1 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[0]})

                for i in range(1, len(trackpoint_id_list)):
                    track_point_2 = self.db[TRACKPOINT].find_one({"_id": trackpoint_id_list[i]})
                    
                    timedifference = datetime.timedelta(minutes=5)
                    if track_point_2["date_time"] - track_point_1["date_time"] > timedifference:
                        if user["_id"] in user_and_number_of_invalid_activities_dict.keys():
                            user_and_number_of_invalid_activities_dict[user["_id"]] = user_and_number_of_invalid_activities_dict[user["_id"]] + 1 
                        else:
                            user_and_number_of_invalid_activities_dict[user["_id"]] = 1

                        break
                    track_point_1 = track_point_2.copy()

        key_value_list = [(key, value) for key, value in user_and_number_of_invalid_activities_dict.items()]

        print(tabulate(key_value_list, headers=["id", "invalid activities"]))

    
    def task10(self):
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397

        user_with_activities_in_forbidden_city = []
        
        user_list = self.db[USER].find({})
        forbidden_city_list = self.db[TRACKPOINT].find({
            "lat": { "$lte" : forbidden_city_lat + 0.001, "$gte" : forbidden_city_lat - 0.001},
            "lon": { "$lte" : forbidden_city_lon + 0.001, "$gte" : forbidden_city_lon - 0.001}
        })


        for user in user_list:
            for activity_id in user["activities"]:
                if user["_id"] in user_with_activities_in_forbidden_city:
                    break

                activity = self.db[ACTIVITY].find_one({"_id": activity_id})
                trackpoint_id_list = activity["trackpoints"]

                
                for track_point_id in trackpoint_id_list:
                    
                    for track_point in forbidden_city_list:
                        if track_point["_id"] == track_point_id:
                            user_with_activities_in_forbidden_city.append(user["_id"])
                            break
        
        print(user_with_activities_in_forbidden_city)
    
    def task11(self):
        users_and_most_used_transportation_mode = []

        user_list = self.db[USER].find({})

        for user in user_list:
            number_per_transportationmode = {}

            for activity_id in user["activities"]:
                activity = self.db[ACTIVITY].find_one({"_id": activity_id})
                if activity["type"] in number_per_transportationmode.keys():
                    number_per_transportationmode[activity["type"]] = number_per_transportationmode[activity["type"]] + 1
                else:
                    number_per_transportationmode[activity["type"]] = 1

            max_count = 0
            max_type = None

            for transportation_mode, count in number_per_transportationmode.items():
                if count > max_count:
                    max_count = count 
                    max_type = transportation_mode
            
            users_and_most_used_transportation_mode.append((user["_id"], max_type))

        users_and_most_used_transportation_mode.sort(key=lambda x : x[0])

        print(tabulate(users_and_most_used_transportation_mode, headers=["user id", "type"]))

    def test(self):
        user_list=self.db[USER].find({})
        print(len(user_list))

                    

if __name__ == "__main__":
    task = Task2()
    # task.task1()
    # task.task2()
    # task.task3()
    # task.task4()
    # task.task5()
    # task.task6a()
    # task.task6b()
    # task.task7()
    # task.task8()

    # Task 9  tar litt tid å køyre
    # task.task9()
    # task.task10()
    # task.task11()
    task.test()