from pprint import pprint
from DbConnector import DbConnector
import os
import time
from bson import objectid


class Part1:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def clean_users(self, user_list, linked_activities, path="dataset/Data/"):
        user_insert = []
        for user_id in user_list:
            has_label = 0
            if "labels.txt" in os.listdir(path+user_id):
                has_label = 1
            if len(linked_activities[user_id]):
                user_insert.append({"_id": user_id, "has_labels": has_label, "activities": linked_activities[user_id]})
            else:
                user_insert.append({"_id": user_id, "has_labels": has_label})
        return user_insert  # Should return a list of user_input_data with {"_id": "000", "has_labels": 0} ...

    def clean_activities(self, path, user):
        activities = []
        user_activities = []
        tps = []
        user_path = path + user
        user_folder = os.listdir(user_path)
        transportation_labels = {}
        if "labels.txt" in user_folder:  # Trajectory, labels.txt?
            with open(user_path + "/labels.txt", 'r') as file:
                next(file)
                for line in file:
                    line = line.strip().split("\t")
                    transportation_labels[line[0]] = line[2]
                    transportation_labels[line[1]] = line[2]
        trajectory_folder = os.listdir(user_path + "/Trajectory/")
        for i, activity in enumerate(trajectory_folder):
            related_tps = []
            with open(user_path + "/Trajectory/" + str(activity)) as file:
                for j, line in enumerate(file):
                    pass
                if j > 2506:
                    continue
            a_id = objectid.ObjectId()
            with open(user_path + "/Trajectory/" + str(activity)) as file:
                for line_no, line in enumerate(file):
                    if line_no < 6:
                        continue
                    if line_no == 6:
                        start = line.strip().split(",")
                        start_date_time = start[-2].replace("-", "/") + " " + start[-1]
                    tp_data = line.strip().split(",")
                    new_id = objectid.ObjectId()
                    related_tps.append(new_id)
                    tp = {
                        "_id": new_id,
                        "lat": float(tp_data[0]),
                        "lng": float(tp_data[1]),
                        "altitude": int(round(float(tp_data[3]))),
                        "date_days": float(tp_data[4]),
                        "date_time": tp_data[5] + " " + tp_data[6],
                        "activity": a_id
                    }
                    tps.append(tp)
                end = line.strip().split(",")
                end_date_time = end[-2].replace("-", "/") + " " + end[-1]
                transportation_mode = None
                if start_date_time in transportation_labels:
                    transportation_mode = transportation_labels[start_date_time]
                if end_date_time in transportation_labels:
                    transportation_mode = transportation_labels[end_date_time]

                if len(related_tps):
                    activity = {
                        "_id": a_id,
                        "transportation_mode": transportation_mode,
                        "start_date_time": start_date_time,
                        "end_date_time": end_date_time,
                        "trackpoints": related_tps,
                        "user": user
                    }
                    activities.append(activity)
                    user_activities.append(a_id)
                file.close()
        return tps, activities, user_activities

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def insert_documents(self, collection_name, docs):
        collection = self.db[collection_name]
        collection.insert_many(docs)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


def main():
    program = None
    data_path = "dataset/Data/"
    user_list = os.listdir(data_path)
    try:
        program = Part1()
        program.drop_coll(collection_name="User")
        program.drop_coll(collection_name="Activity")
        program.drop_coll(collection_name="Trackpoint")
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="Trackpoint")
        program.show_coll()
        users_activity_linked = {}
        for i, user in enumerate(user_list):
            tps, activities, user_activities = program.clean_activities(data_path, user)
            if tps and activities:
                program.insert_documents(collection_name="Trackpoint", docs=tps)
                program.insert_documents(collection_name="Activity", docs=activities)
            users_activity_linked[user] = user_activities
            print("Progress: ", 100 * round((i+1) / 181, 3), " %")
        users = program.clean_users(user_list, users_activity_linked, path=data_path)
        program.insert_documents(collection_name="User", docs=users)
        program.fetch_documents(collection_name="User")
        # program.drop_coll(collection_name="User")
        # program.drop_coll(collection_name="Activity")
        # program.drop_coll(collection_name="Trackpoint")
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
