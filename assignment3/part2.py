from DbConnector import DbConnector

from tabulate import tabulate
from haversine import haversine
import datetime
from pprint import pprint


class Part2:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def get_table(self, table_name):
        collection = self.db[table_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)
        return collection

    def get_number_of_document(self, table_name):
        collection = self.db[table_name]
        count = collection.count_documents({})
        print(count)
        return count

    def get_average_number_of_activities(self):
        nr_users = self.get_number_of_document("User")
        nr_activities = self.get_number_of_document("Activity")
        return nr_activities / nr_users

    def get_top20_users_with_most_activities(self):
        collection = self.db.Activity.aggregate([
            {
                '$group':
                    {
                        '_id': '$user',
                        'activity_count': {'$sum': 1}
                    }
            },
            {
                '$sort': {"activity_count": -1}

            },
            {
                '$limit': 20
            }

        ])
        for user in collection:
            print(user)
        return collection

    def get_taxi_users(self):
        result = self.db.Activity.aggregate([

            {'$match': {"transportation_mode": "taxi"}
             },
            {
                '$group':
                    {
                        '_id': '$user'

                    }
            }

        ])
        for user in result:
            print(user)
        return result

    def get_num_transportation_modes(self):
        result = self.db.Activity.aggregate([

            {'$match': {"transportation_mode": {'$ne': None}}

             },
            {
                '$group':
                    {
                        '_id': '$transportation_mode',
                        'nr_activities': {'$sum': 1}
                    }
            }

        ])
        for tm in result:
            print(tm)
        return result

    def get_year_test(self):
        result = self.db.Activity.aggregate([{

            '$group': {
                '_id':
                    {'$year': {
                        'date': {
                            '$dateFromString': {
                                'dateString': '$start_date_time',
                                'format': '%Y/%m/%d %H:%M:%S'
                            }
                        }
                    }
                    }
            }
        }])

        for obj in result:
            for key, value in obj.items():
                print(key)
                print(value)
                print(type(value))

            break

        return result

    def get_year_with_most_activities(self):
        result = self.db.Activity.aggregate([
            {
                '$group': {
                    '_id': {
                        '$year': {
                            'date': {
                                '$dateFromString': {
                                    'dateString': '$start_date_time',
                                    'format': '%Y/%m/%d %H:%M:%S'
                                }
                            }
                        }
                    },
                    'nr_activities': {'$sum': 1}
                }
            },

            {
                '$sort': {'nr_activities': -1}
            },

            {
                '$limit': 1
            }
        ])
        for year in result:
            print(year)
        return result

    def get_year_with_most_recorded_hours(self):
        result = self.db.Activity.aggregate([

            {
                '$group': {
                    '_id': {
                        '$year': {
                            'date': {
                                '$dateFromString': {
                                    'dateString': '$start_date_time',
                                    'format': '%Y/%m/%d %H:%M:%S'
                                }
                            }
                        }
                    },
                    # sum(time_to_sec(timediff(end_date_time,start_date_time )) / 3600)

                    'recorded_hours': {'$sum':
                        {'$divide':
                            [{'$subtract':
                                [
                                    {
                                        '$dateFromString': {
                                            'dateString': '$end_date_time',
                                            'format': '%Y/%m/%d %H:%M:%S'
                                        }
                                    }
                                    ,
                                    {
                                        '$dateFromString': {
                                            'dateString': '$start_date_time',
                                            'format': '%Y/%m/%d %H:%M:%S'
                                        }
                                    }]},
                                60 * 60 * 1000]
                        }
                    }
                }

            },

            {
                '$sort': {'recorded_hours': -1}
            },

            {
                '$limit': 1
            }
        ])
        for year in result:
            print(year)
        return result

    # Find the total distance (in km) walked in 2008, by user with id=112.
    def get_total_distance(self):
        result = self.db.Activity.aggregate([
        #    {'$match': {
        #        "transportation_mode": "walk",
        #        "user": "112",
        #        {"$year": "$start_date_time"}: 2008
        #        }
        #    }
        ])
        for distance in result:
            print(distance)
        return result


    # Find the users who have tracked an activity in the Forbidden City of Beijing.
    # In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397.
    def get_users_in_Bejing(self):
        result = self.db.Trackpoint.aggregate([
            {
                '$project':
                    {
                        'activity_id': '$_id',
                        'modified_lng': {'$round': ['$lng', 3]},
                        'modified_lat': {'$round': ['$lat', 3]},
                    }
            },
            {
                '$match':
                    {'$and': [
                        {'modified_lng': 116.397},
                        {'modified_lat': 39.916}
                    ]}
            },
            {
                '$group':
                    {
                        '_id': '$_id'
                    }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': 'activity_id',
                    'foreignField': 'activity',
                    'as': 'activity'
                }
            },
            {
                '$unwind': '$activity'
            },
            {
                '$group':
                    {
                        '_id': '$activity.user'
                    }
            }
        ])
        for res in result:
            print(res)
        return result

    def test_lookup(self):

        result = self.db.Trackpoint.aggregate([

            {'$lookup': {
                'from': 'Activity',
                'localField': 'activity',
                'foreignField': '_id',
                'as': 'combined'
            }}
        ])
        print(result)
        for r in result:
            print(r["combined"])

        return result

    def get_most_used_transportation_mode(self):
        result = self.db.Activity.aggregate([
            {  # Filtering out missing values
                '$match': {
                    "transportation_mode": {
                        '$ne': None,
                    }
                }
            },
            {
                '$group':
                    {
                        '_id': {
                            'user': '$user',
                            'mode': '$transportation_mode'
                        },
                        'count': {'$sum': 1}
                    }
            },
            {
                '$group':
                    {
                        '_id': '$_id.user',
                        'mode': {'$first': '$_id.mode'},
                        'cnt': {'$max': '$count'}
                    }
            },
            {
                '$project':
                    {
                        '_id': 0,
                        'user_id': '$_id',
                        'most_used_transportation_mode': '$mode',
                    }
            },
            {
                '$sort':
                    {
                        'user_id': 1
                    }
            }

        ])

        for res in result:
            print(res)
        return

    def test_projection(self):
        result = self.db.Activity.aggregate([

            {
                '$project':
                    {
                        'transportation_mode': 1

                    }
            }

        ])
        for user in result:
            print(user)
        return result


def main():
    program = None
    try:
        ##### Initialize #####
        program = Part2()
        ##### Queries #####

        # ---- Query 1 ----#
        #result = program.get_number_of_document("User")
        # print(result)
        # result2 = program.get_number_of_document("Activity")
        # result3 = program.get_number_of_document("Trackpoint")

        # ---- Query 2 ----#
        # result = program.get_average_number_of_activities()
        # print(result)

        # ---- Query 3 ----#
        #result = program.get_top20_users_with_most_activities()

        # ---- Query 4 ----#
        #answer = program.get_taxi_users()

        # ---- Query 5 ----#
        #answer = program.get_num_transportation_modes()

        # ---- Query 6 ----#
        ########6A##########
        #answer = program.get_year_with_most_activities()

        ########6B##########
        #answer=program.get_year_with_most_recorded_hours()

        # ---- Query 7 ----#
        result = program.get_total_distance()
        #not implemented yet

        # ---- Query 8 ----#
        #not implemented yet

        # ---- Query 9 ----#
        #not implemented yet

        # ---- Query 10 ----#
        # program.get_users_in_Bejing()

        # ---- Query 11 ----#
        #program.get_most_used_transportation_mode()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
