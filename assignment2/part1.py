import os
import time
from DbConnector import DbConnector
from tabulate import tabulate


class Part1Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_users_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   user_id VARCHAR(3) NOT NULL PRIMARY KEY,
                   has_label TINYINT)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_activities_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   activity_id INT PRIMARY KEY,
                   user_id VARCHAR(3),
                   FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                   transportation_mode VARCHAR(255),
                   start_date_time DATETIME NOT NULL,
                   end_date_time DATETIME NOT NULL
                    )
                """
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   tp_id INT AUTO_INCREMENT PRIMARY KEY,
                   activity_id INT,
                   FOREIGN KEY (activity_id) REFERENCES activities(activity_id) ON DELETE CASCADE,
                   lat DOUBLE,
                   lng DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME
                    )
                """
        self.cursor.execute(query % table_name)
        self.db_connection.commit()


    def clean_users(self, path):

        user_insert = []
        for user_id in range(182):
            if user_id < 10:
                user_insert.append([f"00{user_id}", 0])
            elif 9 < user_id < 100:
                user_insert.append([f"0{user_id}", 0])
            else:
                user_insert.append([f"{user_id}", 0])
        file = open(path, "r")
        label_list = [labeled_id.strip() for labeled_id in file]
        for label_id in label_list:
            user_insert[int(label_id)][1] = 1

        for i in range(182):
            user_insert[i] = tuple(user_insert[i])
        # Should return a list of user_input_data with ("id", has_label)
        file.close()
        return user_insert

    def clean_activities_tps(self, path, user_list):
        user_activities = []
        trackpoint_data = []
        activity_id = 1
        for progress, user in enumerate(user_list):
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
                with open(user_path + "/Trajectory/" + str(activity)) as file:
                    for j, line in enumerate(file):
                        pass
                    if j > 2506:
                        continue
                with open(user_path + "/Trajectory/" + str(activity)) as file:
                    lines = file.readlines()
                    start = lines[6].strip().split(",")
                    end = lines[-1].strip().split(",")
                    start_date_time = start[-2].replace("-", "/") + " " + start[-1]
                    end_date_time = end[-2].replace("-", "/") + " " + end[-1]
                    if start_date_time in transportation_labels:
                        transportation_mode = transportation_labels[start_date_time]
                    elif end_date_time in transportation_labels:
                        transportation_mode = transportation_labels[end_date_time]
                    else:
                        transportation_mode = None
                    user_activities.append((activity_id, user, transportation_mode, start_date_time, end_date_time))
                    for line in lines[6:]:
                        line = line.strip().split(",")
                        lat, lng = float(line[0]), float(line[1])
                        altitude = int(round(float(line[3])))
                        date_days = float(line[4])
                        date_time = line[5] + " " + line[6]
                        trackpoint_data.append((activity_id, lat, lng, altitude, date_days, date_time))

                    file.close()
                    activity_id += 1
            print("Progress: ", 100 * round(progress / 181, 3), " %")
        return user_activities, trackpoint_data

    def insert_users(self, users):
        query = "INSERT INTO users (user_id, has_label) VALUES (%s, %s)"

        self.cursor.executemany(query, users)
        self.db_connection.commit()

    def insert_activities(self, activities):
        query = "INSERT INTO activities (activity_id, user_id, transportation_mode, start_date_time, end_date_time) " \
                "VALUES (%s, %s, %s, %s, %s)"

        self.cursor.executemany(query, activities)
        self.db_connection.commit()

    def insert_trackpoints(self, tps):
        query = "INSERT INTO trackpoints (activity_id, lat, lng, altitude, date_days, date_time)" \
                "VALUES (%s, %s, %s, %s, %s, %s)"

        self.cursor.executemany(query, tps)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    labeled_path = "dataset/labeled_ids.txt"
    users_path = "dataset/Data/"
    users_list = os.listdir(users_path)  # ["000", "001", ...]
    program = Part1Program()
    users_data = program.clean_users(labeled_path)
    activities_data, tp_data = program.clean_activities_tps(users_path, users_list)
    batch_size = 1024

    try:
        program.create_users_table(table_name="users")
        program.insert_users(users_data)
    except Exception as e:
        print("USER ERROR: Failed to use database:", e)
    try:
        program.create_activities_table(table_name="activities")
        tracker = 0
        while tracker + batch_size < len(activities_data):
            start = time.time()
            program.insert_activities(activities_data[tracker:tracker+batch_size])
            end = time.time()
            tracker += batch_size
            print("time", end-start, tracker)
        program.insert_activities(activities_data[tracker:])
    except Exception as e:
        print("ACTIVITY ERROR: Failed to use database:", e)
    # try:
    #     program.create_trackpoint_table(table_name="trackpoints")
    #     tracker = 0
    #     while tracker + batch_size < len(tp_data):
    #         start = time.time()
    #         program.insert_trackpoints(tp_data[tracker:tracker + batch_size])
    #         end = time.time()
    #         tracker += batch_size
    #         print(tracker, "time", end-start)
    #     program.insert_trackpoints(tp_data[tracker:])
    # except Exception as e:
    #     print("TP ERROR: Failed to use database:", e)
    finally:
        _ = program.fetch_data(table_name="activities")
#        program.drop_table(table_name="activities")
#        program.drop_table(table_name="users")
        # Check that the table is dropped
        program.show_tables()
    if program:
        program.connection.close_connection()


if __name__ == '__main__':
    main()
